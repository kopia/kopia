package repositorystress_test

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/tests/repository_stress_test/repomodel"
)

const (
	shortStressTestDuration = 10 * time.Second
	longStressTestDuration  = 120 * time.Second
)

type actName string

const (
	actWriteRandomContent = "writeRandomContent"
	actReadPendingContent = "readPendingContent"
	actReadFlushContent   = "readFlushedContent"

	actListContents           = "listContents"
	actListAndReadAllContents = "listAndReadAllContents"

	actCompact = "compact"
	actFlush   = "flush"
	actRefresh = "refresh"

	actReadPendingManifest = "readPendingManifest"
	actReadFlushedManifest = "readFlushedManifest"
	actWriteRandomManifest = "writeRandomManifest"
)

var actions = map[actName]func(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error{
	actWriteRandomContent:     writeRandomContent,
	actReadPendingContent:     readPendingContent,
	actReadFlushContent:       readFlushedContent,
	actListContents:           listContents,
	actListAndReadAllContents: listAndReadAllContents,
	actCompact:                compact,
	actFlush:                  flush,
	actRefresh:                refresh,
	actReadPendingManifest:    readPendingManifest,
	actReadFlushedManifest:    readFlushedManifest,
	actWriteRandomManifest:    writeRandomManifest,
}

type StressOptions struct {
	ConfigsPerRepository      int
	OpenRepositoriesPerConfig int
	SessionsPerOpenRepository int
	WorkersPerSession         int

	ActionWeights map[actName]int
}

var errSkipped = errors.New("skipped")

const masterPassword = "foo-bar-baz-1234"

func TestStressRepositoryMixAll(t *testing.T) {
	runStress(t, &StressOptions{
		ConfigsPerRepository:      2,
		OpenRepositoriesPerConfig: 2,
		SessionsPerOpenRepository: 2,
		WorkersPerSession:         2,

		ActionWeights: map[actName]int{
			actWriteRandomContent:     10,
			actReadPendingContent:     500,
			actReadFlushContent:       500,
			actListContents:           10,
			actListAndReadAllContents: 10,
			actCompact:                2,
			actFlush:                  10,
			actRefresh:                20,
			actReadPendingManifest:    300,
			actReadFlushedManifest:    300,
			actWriteRandomManifest:    20,
		},
	})
}

func TestStressRepositoryRandomMix(t *testing.T) {
	runStress(t, &StressOptions{
		ConfigsPerRepository:      1 + rand.Intn(3),
		OpenRepositoriesPerConfig: 1 + rand.Intn(2),
		SessionsPerOpenRepository: 1 + rand.Intn(2),
		WorkersPerSession:         1 + rand.Intn(2),

		ActionWeights: map[actName]int{
			actWriteRandomContent:     1 + rand.Intn(100),
			actReadPendingContent:     1 + rand.Intn(100),
			actReadFlushContent:       1 + rand.Intn(100),
			actListContents:           1 + rand.Intn(100),
			actListAndReadAllContents: 1 + rand.Intn(100),
			actCompact:                1 + rand.Intn(100),
			actFlush:                  1 + rand.Intn(100),
			actRefresh:                1 + rand.Intn(100),
			actReadPendingManifest:    1 + rand.Intn(100),
			actReadFlushedManifest:    1 + rand.Intn(100),
			actWriteRandomManifest:    1 + rand.Intn(100),
		},
	})
}

func TestStressRepositoryManifests(t *testing.T) {
	runStress(t, &StressOptions{
		ConfigsPerRepository:      1,
		OpenRepositoriesPerConfig: 1,
		SessionsPerOpenRepository: 2,
		WorkersPerSession:         1,

		ActionWeights: map[actName]int{
			actCompact:             2,
			actFlush:               10,
			actRefresh:             20,
			actReadPendingManifest: 300,
			actReadFlushedManifest: 300,
			actWriteRandomManifest: 1,
		},
	})
}

func TestStressContentWriteHeavy(t *testing.T) {
	runStress(t, &StressOptions{
		ConfigsPerRepository:      2,
		OpenRepositoriesPerConfig: 2,
		SessionsPerOpenRepository: 2,
		WorkersPerSession:         2,

		ActionWeights: map[actName]int{
			actWriteRandomContent:     500,
			actReadPendingContent:     10,
			actReadFlushContent:       10,
			actListContents:           10,
			actListAndReadAllContents: 10,
			actCompact:                2,
			actFlush:                  10,
			actRefresh:                20,
		},
	})
}

func TestStressContentReadHeavy(t *testing.T) {
	runStress(t, &StressOptions{
		ConfigsPerRepository:      2,
		OpenRepositoriesPerConfig: 2,
		SessionsPerOpenRepository: 2,
		WorkersPerSession:         2,

		ActionWeights: map[actName]int{
			actWriteRandomContent:     5,
			actReadPendingContent:     500,
			actReadFlushContent:       500,
			actListContents:           500,
			actListAndReadAllContents: 10,
			actCompact:                2,
			actFlush:                  10,
			actRefresh:                20,
		},
	})
}

//nolint:thelper
func runStress(t *testing.T, opt *StressOptions) {
	if testing.Short() {
		return
	}

	if os.Getenv("KOPIA_STRESS_TEST") == "" {
		t.Skip("skipping stress test")
	}

	t.Logf("running stress test with options: %#v", *opt)

	ctx := testlogging.Context(t)

	tmpPath, err := os.MkdirTemp("", "kopia")
	if err != nil {
		t.Fatalf("unable to create temp directory")
	}

	defer func() {
		if !t.Failed() {
			os.RemoveAll(tmpPath)
		}
	}()

	t.Logf("path: %v", tmpPath)

	storagePath := filepath.Join(tmpPath, "storage")

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: storagePath,
	}, true)
	if err != nil {
		t.Fatalf("unable to initialize storage: %v", err)
	}

	// create repository
	if err = repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, masterPassword); err != nil {
		t.Fatalf("unable to initialize repository: %v", err)
	}

	var configFiles []string

	// set up two parallel kopia connections, each with its own config file and cache.
	for i := range opt.ConfigsPerRepository {
		configFile := filepath.Join(tmpPath, fmt.Sprintf("kopia-%v.config", i))
		configFiles = append(configFiles, configFile)

		if err = repo.Connect(ctx, configFile, st, masterPassword, &repo.ConnectOptions{
			CachingOptions: content.CachingOptions{
				CacheDirectory:        filepath.Join(tmpPath, fmt.Sprintf("cache-%v", i)),
				ContentCacheSizeBytes: 2000000000,
			},
		}); err != nil {
			t.Fatalf("unable to connect %v: %v", configFile, err)
		}
	}

	var stop atomic.Bool

	eg, ctx := errgroup.WithContext(ctx)

	logDir := testutil.TempLogDirectory(t)

	rm := repomodel.NewRepositoryData()

	logFileName := filepath.Join(logDir, "workers.log")
	logFile, err := os.Create(logFileName)
	require.NoError(t, err)
	t.Logf("log file: %v", logFileName)

	defer logFile.Close()

	for _, configFile := range configFiles {
		for i := range opt.OpenRepositoriesPerConfig {
			openID := fmt.Sprintf("open-%v", i)

			eg.Go(func() error {
				log := testlogging.Printf(func(msg string, args ...interface{}) {
					fmt.Fprintf(logFile, clock.Now().Format("2006-01-02T15:04:05.000000Z07:00")+" "+msg+"\n", args...)
				}, "").With("cfg", fmt.Sprintf("%v::o%v", filepath.Base(configFile), i))

				ctx2 := logging.WithLogger(ctx, func(module string) logging.Logger {
					return log
				})

				return longLivedRepositoryTest(ctx2, t, openID, configFile, rm, log, opt, &stop)
			})
		}
	}

	duration := shortStressTestDuration
	if os.Getenv("CI") != "" && os.Getenv("IS_PULL_REQUEST") == "false" {
		duration = longStressTestDuration
	}

	time.Sleep(duration)
	stop.Store(true)

	require.NoError(t, eg.Wait())
}

func longLivedRepositoryTest(ctx context.Context, t *testing.T, openID, configFile string, rm *repomodel.RepositoryData, log logging.Logger, opt *StressOptions, stop *atomic.Bool) error {
	t.Helper()

	// important to call OpenRepository() before repo.Open() to ensure we're not seeing state
	// added between repo.Open() and OpenRepository()
	or := rm.OpenRepository(openID)

	rep, err := repo.Open(ctx, configFile, masterPassword, &repo.Options{})
	if err != nil {
		return errors.Wrap(err, "error opening repository")
	}

	defer rep.Close(ctx)

	eg, ctx := errgroup.WithContext(ctx)

	for i := range opt.SessionsPerOpenRepository {
		ors := or.NewSession(fmt.Sprintf("session-%v", i))

		_, w, err := rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{
			Purpose: fmt.Sprintf("longLivedRepositoryTest-w%v", i),
		})
		if err != nil {
			return errors.Wrap(err, "error opening writer")
		}

		for j := range opt.WorkersPerSession {
			log2 := log.With("worker", fmt.Sprintf("s%vw%v::", i, j))

			eg.Go(func() error {
				return repositoryTest(ctx, t, stop, w, ors, log2, opt)
			})
		}
	}

	return eg.Wait()
}

func repositoryTest(ctx context.Context, t *testing.T, stop *atomic.Bool, rep repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger, opt *StressOptions) error {
	t.Helper()

	var totalWeight int
	for _, w := range opt.ActionWeights {
		totalWeight += w
	}

	for ctx.Err() == nil && !stop.Load() {
		roulette := rand.Intn(totalWeight)
		for act, weight := range opt.ActionWeights {
			if roulette < weight {
				if err := actions[act](ctx, rep, rs, log); err != nil {
					if errors.Is(err, errSkipped) {
						break
					}

					log.Errorf("FAILED %v: %v", act, err)

					return errors.Wrapf(err, "error running %v", act)
				}

				log.Errorf("SUCCEEDED %v", act)

				break
			}

			roulette -= weight
		}
	}

	return nil
}

func writeRandomContent(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	data := make([]byte, 1000)
	cryptorand.Read(data)

	contentID, err := r.ContentManager().WriteContent(ctx, gather.FromSlice(data), "", content.NoCompression)
	if err != nil {
		return errors.Wrap(err, "WriteContent error")
	}

	log.Debugf("writeRandomContent(%v,%x)", contentID, data[0:16])

	rs.WriteContent(ctx, contentID)

	return errors.Wrapf(err, "writeRandomContent(%v)", contentID)
}

func readPendingContent(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	contentID := rs.WrittenContents.PickRandom(ctx)
	if contentID == content.EmptyID {
		return errSkipped
	}

	log.Debugf("readPendingContent(%v)", contentID)

	_, err := r.ContentReader().GetContent(ctx, contentID)
	if err == nil {
		return nil
	}

	return errors.Wrapf(err, "readPendingContent(%v)", contentID)
}

func readFlushedContent(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	contentID := rs.OpenRepo.ReadableContents.PickRandom(ctx)
	if contentID == content.EmptyID {
		return errSkipped
	}

	log.Debugf("readFlushedContent(%v)", contentID)

	_, err := r.ContentReader().GetContent(ctx, contentID)
	if err == nil {
		return nil
	}

	return errors.Wrapf(err, "readFlushedContent(%v)", contentID)
}

func listContents(ctx context.Context, r repo.DirectRepositoryWriter, _ *repomodel.RepositorySession, log logging.Logger) error {
	log.Debug("listContents()")

	return errors.Wrapf(r.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{},
		func(i content.Info) error { return nil },
	), "listContents()")
}

func listAndReadAllContents(ctx context.Context, r repo.DirectRepositoryWriter, _ *repomodel.RepositorySession, log logging.Logger) error {
	log.Debug("listAndReadAllContents()")

	return errors.Wrapf(r.ContentReader().IterateContents(
		ctx,
		content.IterateOptions{},
		func(ci content.Info) error {
			cid := ci.ContentID
			_, err := r.ContentReader().GetContent(ctx, cid)
			if err != nil {
				return errors.Wrapf(err, "error reading content %v", cid)
			}

			return nil
		}), "listAndReadAllContents()")
}

func compact(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	if !rs.OpenRepo.EnableMaintenance {
		return errSkipped
	}

	log.Debug("compact()")

	return errors.Wrapf(
		r.ContentManager().CompactIndexes(ctx, indexblob.CompactOptions{MaxSmallBlobs: 1}),
		"compact()")
}

func flush(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	log.Debug("flush()")

	// capture contents and manifests we had before we start flushing.
	// this is necessary since operations can proceed in parallel to Flush() which might add more data
	// to the model. It would be incorrect to flush the latest state of the model
	// because we don't know for sure if the corresponding repository data has indeed been flushed.
	wc := rs.WrittenContents.Snapshot("")
	wm := rs.WrittenManifests.Snapshot("")

	if err := r.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing")
	}

	// flush model after flushing the repository to communicate to other sessions that they can expect
	// to see flushed items now.
	rs.Flush(ctx, wc, wm)

	return nil
}

func refresh(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	log.Debug("refresh()")

	// refresh model before refreshing repository to guarantee that repository has at least all the items in
	// the model (possibly more).
	cids := rs.OpenRepo.RepoData.CommittedContents.Snapshot("")
	mids := rs.OpenRepo.RepoData.CommittedManifests.Snapshot("")

	if err := r.Refresh(ctx); err != nil {
		return errors.Wrap(err, "refresh error")
	}

	rs.Refresh(ctx, cids, mids)

	return nil
}

func readPendingManifest(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	manifestID := rs.WrittenManifests.PickRandom(ctx)
	if manifestID == "" {
		return errSkipped
	}

	log.Debugf("readPendingManifest(%v)", manifestID)

	_, err := r.GetManifest(ctx, manifestID, nil)
	if err == nil {
		return nil
	}

	return errors.Wrapf(err, "readPendingManifest(%v)", manifestID)
}

func readFlushedManifest(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	manifestID := rs.OpenRepo.ReadableManifests.PickRandom(ctx)
	if manifestID == "" {
		return errSkipped
	}

	log.Debugf("readFlushedManifest(%v)", manifestID)

	_, err := r.GetManifest(ctx, manifestID, nil)
	if err == nil {
		return nil
	}

	return errors.Wrapf(err, "readFlushedManifest(%v)", manifestID)
}

func writeRandomManifest(ctx context.Context, r repo.DirectRepositoryWriter, rs *repomodel.RepositorySession, log logging.Logger) error {
	key1 := fmt.Sprintf("key-%v", rand.Intn(10))
	key2 := fmt.Sprintf("key-%v", rand.Intn(10))
	val1 := fmt.Sprintf("val1-%v", rand.Intn(10))
	val2 := fmt.Sprintf("val2-%v", rand.Intn(10))
	content1 := fmt.Sprintf("content-%v", rand.Intn(10))
	content2 := fmt.Sprintf("content-%v", rand.Intn(10))
	content1val := fmt.Sprintf("val1-%v", rand.Intn(10))
	content2val := fmt.Sprintf("val2-%v", rand.Intn(10))

	mid, err := r.PutManifest(ctx, map[string]string{
		"type": key1,
		key1:   val1,
		key2:   val2,
	}, map[string]string{
		content1: content1val,
		content2: content2val,
	})
	if err != nil {
		return err
	}

	log.Debugf("writeRandomManifest(%v)", mid)
	rs.WriteManifest(ctx, mid)

	return err
}
