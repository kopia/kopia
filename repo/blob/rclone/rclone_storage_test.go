package rclone_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/rclone"
	"github.com/kopia/kopia/repo/blob/sharded"
)

const cleanupAge = 4 * time.Hour

var rcloneExternalProviders = map[string]string{
	"GoogleDrive": "gdrive:/kopia",
	"OneDrive":    "onedrive:/kopia",
	"DropBox":     "dropbox:/kopia",
}

func mustGetRcloneExeOrSkip(t *testing.T) string {
	t.Helper()

	rcloneExe := os.Getenv("RCLONE_EXE")
	if rcloneExe == "" {
		rcloneExe = "rclone"
	}

	if err := exec.Command(rcloneExe, "version").Run(); err != nil {
		if os.Getenv("CI") == "" {
			t.Skipf("rclone not installed: %v", err)
		} else {
			// on CI fail hard
			t.Fatalf("rclone not installed: %v", err)
		}
	}

	t.Logf("using rclone exe: %v", rcloneExe)

	return rcloneExe
}

func TestRCloneStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	rcloneExe := mustGetRcloneExeOrSkip(t)
	dataDir := testutil.TempDirectory(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	st, err := rclone.New(newctx, &rclone.Options{
		// pass local file as remote path.
		RemotePath: dataDir,
		RCloneExe:  rcloneExe,
	}, true)

	cancel()

	if err != nil {
		t.Fatalf("unable to connect to rclone backend: %v", err)
	}

	defer st.Close(ctx)

	var eg errgroup.Group

	// trigger multiple parallel reads to ensure we're properly preventing race
	// described in https://github.com/kopia/kopia/issues/624
	for range 100 {
		eg.Go(func() error {
			var tmp gather.WriteBuffer
			defer tmp.Close()

			if err := st.GetBlob(ctx, blob.ID(uuid.New().String()), 0, -1, &tmp); !errors.Is(err, blob.ErrBlobNotFound) {
				return errors.Errorf("unexpected error when downloading non-existent blob: %v", err)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
}

func TestRCloneStorageDirectoryShards(t *testing.T) {
	t.Parallel()

	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	rcloneExe := mustGetRcloneExeOrSkip(t)
	dataDir := testutil.TempDirectory(t)

	st, err := rclone.New(ctx, &rclone.Options{
		// pass local file as remote path.
		RemotePath: dataDir,
		RCloneExe:  rcloneExe,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	if err != nil {
		t.Fatalf("unable to connect to rclone backend: %v", err)
	}

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	require.FileExists(t, filepath.Join(dataDir, "someb", "lo", "b1234567812345678.f"))
}

type Killable interface {
	Kill()
}

func TestRCloneStorageInvalidExe(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	_, err := rclone.New(ctx, &rclone.Options{
		RCloneExe:  "no-such-rclone",
		RemotePath: "mmm:/tmp/rclonetest",
	}, false)
	if err == nil {
		t.Fatalf("unexpected success wen starting rclone")
	}
}

func TestRCloneStorageInvalidFlags(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	_, err := rclone.New(ctx, &rclone.Options{
		RCloneExe:  mustGetRcloneExeOrSkip(t),
		RemotePath: "mmm:/tmp/rclonetest",
		RCloneArgs: []string{"--no-such-flag"},
	}, false)
	if err == nil {
		t.Fatalf("unexpected success wen starting rclone")
	}

	if !strings.Contains(err.Error(), "--no-such-flag") {
		t.Fatalf("error does not mention invalid flag (got '%v')", err)
	}
}

func TestRCloneProviders(t *testing.T) {
	testutil.ProviderTest(t)

	var (
		rcloneArgs     []string
		embeddedConfig string
	)

	if cfg := os.Getenv("KOPIA_RCLONE_EMBEDDED_CONFIG_B64"); cfg != "" {
		b, err := base64.StdEncoding.DecodeString(cfg)
		if err != nil {
			t.Fatalf("unable to decode KOPIA_RCLONE_EMBEDDED_CONFIG_B64: %v", err)
		}

		embeddedConfig = string(b)
	}

	if cfg := os.Getenv("KOPIA_RCLONE_CONFIG_FILE"); cfg != "" {
		rcloneArgs = append(rcloneArgs, "--config="+cfg)
	}

	testCacheDir := t.TempDir()

	// Try some performance-improving caching, recommended in various places as a way to speed
	// up rclone. Some of them unfortunately enable write-back which does not guarantee full crash
	// consistency required by Kopia.
	//
	// The rclone provider will be ignoring and overriding some of these parameters.
	rcloneArgs = append(rcloneArgs,
		"--vfs-cache-max-size=100M",
		"--vfs-cache-mode=full",
		"--cert", "this-will-be-ignored",
		"--vfs-write-back=30s", // this will be overridden
		"--transfers=16",
		"--cache-dir", testCacheDir,
	)

	if len(rcloneArgs)+len(embeddedConfig) == 0 {
		t.Skipf("Either KOPIA_RCLONE_EMBEDDED_CONFIG_B64 or KOPIA_RCLONE_CONFIG_FILE must be provided")
	}

	rcloneExe := mustGetRcloneExeOrSkip(t)

	for name, rp := range rcloneExternalProviders {
		opt := &rclone.Options{
			RemotePath:     rp,
			RCloneExe:      rcloneExe,
			RCloneArgs:     rcloneArgs,
			EmbeddedConfig: embeddedConfig,
			Debug:          true,
			Options: sharded.Options{
				ListParallelism: 16,
			},
			AtomicWrites: true,
		}

		t.Run("Cleanup-"+name, func(t *testing.T) {
			cleanupOldData(t, rcloneExe, rp)
		})

		t.Run(name, func(t *testing.T) {
			ctx := testlogging.Context(t)

			// we are using shared storage, append a guid so that tests don't collide
			opt.RemotePath += "/" + uuid.NewString()

			st, err := rclone.New(ctx, opt, true)
			require.NoError(t, err)

			k, ok := st.(Killable)
			require.True(t, ok, "not killable")

			blobtesting.VerifyStorage(ctx, t, logging.NewWrapper(st, testlogging.NewTestLogger(t), "[RCLONE-STORAGE] "),
				blob.PutOptions{})

			blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
			require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))

			// write a bunch of tiny blobs massively in parallel
			// and kill rclone immediately after to ensure all writes are synchronous
			var wg sync.WaitGroup

			prefix := uuid.NewString()

			for i := range 10 {
				wg.Add(1)

				go func() {
					defer wg.Done()

					for j := range 3 {
						assert.NoError(t, st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v-%v-%v", prefix, i, j)), gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
					}
				}()
			}

			wg.Wait()
			k.Kill()

			t.Logf("========================")

			// wipe cache
			time.Sleep(3 * time.Second)
			os.RemoveAll(testCacheDir)

			// ensure we can read all blobs written just before killing `rclone`
			st2, err := rclone.New(ctx, opt, false)
			require.NoError(t, err)

			defer st2.Close(ctx)

			var eg errgroup.Group

			for i := range 10 {
				for j := range 3 {
					blobID := blob.ID(fmt.Sprintf("%v-%v-%v", prefix, i, j))

					eg.Go(func() error {
						var buf gather.WriteBuffer
						defer buf.Close()

						return errors.Wrapf(st2.GetBlob(ctx, blobID, 0, -1, &buf), "blob %v", blobID)
					})
				}
			}

			require.NoError(t, eg.Wait())
		})
	}
}

func cleanupOldData(t *testing.T, rcloneExe, remotePath string) {
	t.Helper()

	configFile := os.Getenv("KOPIA_RCLONE_CONFIG_FILE")

	if cfg := os.Getenv("KOPIA_RCLONE_EMBEDDED_CONFIG_B64"); cfg != "" {
		b, err := base64.StdEncoding.DecodeString(cfg)
		if err != nil {
			t.Fatalf("unable to decode KOPIA_RCLONE_EMBEDDED_CONFIG_B64: %v", err)
		}

		tmpDir := testutil.TempDirectory(t)

		configFile = filepath.Join(tmpDir, "rclone.conf")

		//nolint:mnd
		if err = os.WriteFile(configFile, b, 0o600); err != nil {
			t.Fatalf("unable to write config file: %v", err)
		}
	}

	c := exec.Command(rcloneExe, "--config", configFile, "lsjson", remotePath)
	b, err := c.Output()
	require.NoError(t, err)

	var entries []struct {
		IsDir   bool
		Name    string
		ModTime time.Time
	}

	require.NoError(t, json.Unmarshal(b, &entries))

	for _, e := range entries {
		if !e.IsDir {
			continue
		}

		age := clock.Now().Sub(e.ModTime)
		if age > cleanupAge {
			t.Logf("purging: %v %v", e.Name, age)

			if err := exec.Command(rcloneExe, "--config", configFile, "purge", remotePath+"/"+e.Name).Run(); err != nil {
				t.Logf("error purging %v: %v", e.Name, err)
			}
		}
	}
}
