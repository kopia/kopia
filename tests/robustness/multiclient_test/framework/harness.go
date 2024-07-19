//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package framework

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"path"
	"strconv"
	"syscall"
	"testing"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	dataSubPath          = "robustness-data"
	metadataSubPath      = "robustness-metadata"
	contentCacheLimitMB  = 500
	metadataCacheLimitMB = 500
)

// RepoPathPrefix is used by robustness tests as a base dir for repository under test.
var RepoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")

// NewHarness returns a test harness. It requires a context that contains a client.
func NewHarness(ctx context.Context) *TestHarness {
	th := &TestHarness{}
	th.init(ctx)

	return th
}

// TestHarness provides a Kopia robustness.Engine.
type TestHarness struct {
	dataRepoPath string
	metaRepoPath string

	baseDirPath string
	fileWriter  *MultiClientFileWriter
	snapshotter *MultiClientSnapshotter
	persister   *snapmeta.KopiaPersisterLight
	engine      *engine.Engine

	skipTest bool
}

func (th *TestHarness) init(ctx context.Context) {
	if *RepoPathPrefix == "" {
		log.Printf("Skipping robustness tests because repo-path-prefix is not set")
		os.Exit(0)
	}
	dataRepoPath := path.Join(*RepoPathPrefix, dataSubPath)
	metaRepoPath := path.Join(*RepoPathPrefix, metadataSubPath)

	th.dataRepoPath = dataRepoPath
	th.metaRepoPath = metaRepoPath

	// Override ENGINE_MODE env variable. Multiclient tests can only run in SERVER mode.
	log.Printf("Setting %s to %s\n", snapmeta.EngineModeEnvKey, snapmeta.EngineModeServer)

	err := os.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeServer)
	if err != nil {
		log.Printf("Error setting ENGINE_MODE to server: %s", err.Error())
		os.Exit(1)
	}

	// the initialization state machine is linear and bails out on first failure
	if th.makeBaseDir() && th.getFileWriter() && th.getSnapshotter() &&
		th.getPersister() && th.getEngine(ctx) {
		return // success!
	}

	err = th.Cleanup(ctx)
	if err != nil {
		log.Printf("Error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	if th.skipTest {
		os.Exit(0)
	}

	os.Exit(1)
}

func (th *TestHarness) makeBaseDir() bool {
	baseDir, err := os.MkdirTemp("", "engine-data-")
	if err != nil {
		log.Println("Error creating temp dir:", err)
		return false
	}

	th.baseDirPath = baseDir

	return true
}

func (th *TestHarness) getFileWriter() bool {
	if os.Getenv(fio.FioExeEnvKey) == "" && os.Getenv(fio.FioDockerImageEnvKey) == "" {
		log.Println("Skipping robustness tests because FIO environment is not set")

		th.skipTest = true

		return false
	}

	fw := NewMultiClientFileWriter(
		func() (FileWriter, error) { return fiofilewriter.New() },
	)

	th.fileWriter = fw

	return true
}

func (th *TestHarness) getSnapshotter() bool {
	newClientFn := func(baseDirPath string) (ClientSnapshotter, error) {
		return snapmeta.NewSnapshotter(th.baseDirPath)
	}

	s, err := NewMultiClientSnapshotter(th.baseDirPath, newClientFn)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")

			th.skipTest = true
		} else {
			log.Println("Error creating multiclient kopia Snapshotter:", err)
		}

		return false
	}

	th.snapshotter = s

	if err = s.ConnectOrCreateRepo(th.dataRepoPath); err != nil {
		log.Println("Error initializing kopia Snapshotter:", err)

		return false
	}

	// Set size limits for content cache and metadata cache for repository under test.
	if err = s.setCacheSizeLimits(contentCacheLimitMB, metadataCacheLimitMB); err != nil {
		log.Println("Error setting hard cache size limits for kopia snapshotter:", err)

		return false
	}

	return true
}

func (th *TestHarness) getPersister() bool {
	kp, err := snapmeta.NewPersisterLight(th.baseDirPath)
	if err != nil {
		log.Println("Error creating kopia Persister:", err)
		return false
	}

	th.persister = kp

	if err = kp.ConnectOrCreateRepo(th.metaRepoPath); err != nil {
		log.Println("Error initializing kopia Persister:", err)
		return false
	}

	// Set cache size limits for metadata repository.
	if err = kp.SetCacheLimits(th.metaRepoPath, &content.CachingOptions{
		ContentCacheSizeLimitBytes:  500,
		MetadataCacheSizeLimitBytes: 500,
	}); err != nil {
		log.Println("Error setting cache size limits for kopia Persister:", err)
		return false
	}

	return true
}

func (th *TestHarness) getEngine(ctx context.Context) bool {
	args := &engine.Args{
		MetaStore:        th.persister,
		TestRepo:         th.snapshotter,
		FileWriter:       th.fileWriter,
		WorkingDir:       th.baseDirPath,
		SyncRepositories: true,
	}

	eng, err := engine.New(args) //nolint:govet
	if err != nil {
		log.Println("Error on engine creation:", err)
		return false
	}

	// Initialize the engine, connecting it to the repositories.
	// Note that th.engine is not yet set so that metadata will not be
	// flushed on cleanup in case there was an issue while loading.
	err = eng.Init(ctx)
	if err != nil {
		log.Println("Error initializing engine:", err)
		return false
	}

	th.engine = eng

	return true
}

// Engine returns the Kopia robustness test engine.
func (th *TestHarness) Engine() *engine.Engine {
	return th.engine
}

// Run runs the provided function asynchronously for each of the given client
// contexts, waits for all of them to finish, and optionally cleans up clients.
func (th *TestHarness) Run( //nolint:thelper
	ctxs []context.Context,
	t *testing.T, cleanup bool,
	f func(context.Context, *testing.T),
) {
	t.Run("group", func(t *testing.T) {
		testNum := 0

		for _, ctx := range ctxs {
			testNum++

			t.Run(strconv.Itoa(testNum), func(t *testing.T) {
				t.Parallel()
				f(ctx, t)
			})
		}
	})

	if !cleanup {
		return
	}

	for _, ctx := range ctxs {
		th.snapshotter.CleanupClient(ctx)
	}
}

// RunN creates client contexts, runs the provided function asynchronously for
// each client, waits for all of them to finish, and cleans up clients.
func (th *TestHarness) RunN( //nolint:thelper
	ctx context.Context,
	t *testing.T,
	numClients int,
	f func(context.Context, *testing.T),
) {
	ctxs := NewClientContexts(ctx, numClients)
	th.Run(ctxs, t, true, f)
}

// Cleanup shuts down the engine and stops the test app. It requires a context
// that contains the client that was used to initialize the harness.
func (th *TestHarness) Cleanup(ctx context.Context) (retErr error) {
	if th.engine != nil {
		retErr = th.engine.Shutdown(ctx)
	}

	if th.persister != nil {
		th.persister.Cleanup()
	}

	if th.snapshotter != nil {
		if sc := th.snapshotter.ServerCmd(); sc != nil {
			if err := sc.Process.Signal(syscall.SIGTERM); err != nil {
				log.Println("Warning: Failed to send termination signal to kopia server process:", err)
			}
		}

		th.snapshotter.Cleanup()
	}

	if th.fileWriter != nil {
		th.fileWriter.Cleanup()
	}

	if th.baseDirPath != "" {
		err := os.RemoveAll(th.baseDirPath)
		if err != nil {
			log.Printf("Error removing path: %s", err.Error())
		}
	}

	return retErr
}

// GetDirsToLog collects the directory paths to log.
func (th *TestHarness) GetDirsToLog(ctx context.Context) []string {
	if th.snapshotter == nil {
		return nil
	}

	var dirList []string
	dirList = append(dirList,
		th.dataRepoPath, // repo under test base dir
		th.metaRepoPath, // metadata repository base dir
		path.Join(th.fileWriter.DataDirectory(ctx), ".."), // LocalFioDataPathEnvKey
		th.engine.MetaStore.GetPersistDir(),               // kopia-persistence-root-
		th.baseDirPath,                                    // engine-data dir
	)

	cacheDir, _, err := th.snapshotter.GetCacheDirInfo()
	if err == nil {
		dirList = append(dirList, cacheDir) // cache dir for repo under test
	}
	allCacheDirs := getAllCacheDirs(cacheDir)
	dirList = append(dirList, allCacheDirs...)

	return dirList
}

func getAllCacheDirs(dir string) []string {
	if dir == "" {
		return nil
	}
	var dirs []string
	// Collect all cache dirs
	// There are six types of caches, and corresponding dirs.
	// metadata, contents, indexes,
	// own-writes, blob-list, server-contents
	cacheDirSubpaths := []string{"metadata", "contents", "indexes", "own-writes", "blob-list", "server-contents"}
	for _, s := range cacheDirSubpaths {
		dirs = append(dirs, path.Join(dir, s))
	}

	return dirs
}
