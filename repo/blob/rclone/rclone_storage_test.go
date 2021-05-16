package rclone_test

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/rclone"
)

const defaultCleanupAge = time.Hour

var rcloneExternalProviders = map[string]string{
	"GoogleDrive": "gdrive:/kopia",
	// "OneDrive":    "onedrive:/kopia", broken
}

func mustGetRcloneExeOrSkip(t *testing.T) string {
	t.Helper()

	rcloneExe := os.Getenv("RCLONE_EXE")
	if rcloneExe == "" {
		rcloneExe = "rclone"
	}

	if err := exec.Command(rcloneExe, "version").Run(); err != nil {
		if os.Getenv("CI") == "" {
			t.Skip("rclone not installed")
		} else {
			// on CI fail hard
			t.Fatal("rclone not installed")
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

	st, err := rclone.New(ctx, &rclone.Options{
		// pass local file as remote path.
		RemotePath: dataDir,
		RCloneExe:  rcloneExe,
	})
	if err != nil {
		t.Fatalf("unable to connect to rclone backend: %v", err)
	}

	defer st.Close(ctx)

	var eg errgroup.Group

	// trigger multiple parallel reads to ensure we're properly preventing race
	// described in https://github.com/kopia/kopia/issues/624
	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			if _, err := st.GetBlob(ctx, blob.ID(uuid.New().String()), 0, -1); !errors.Is(err, blob.ErrBlobNotFound) {
				return errors.Errorf("unexpected error when downloading non-existent blob: %v", err)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
}

func TestRCloneStorageInvalidExe(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	_, err := rclone.New(ctx, &rclone.Options{
		RCloneExe:  "no-such-rclone",
		RemotePath: "mmm:/tmp/rclonetest",
	})
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
	})
	if err == nil {
		t.Fatalf("unexpected success wen starting rclone")
	}

	if !strings.Contains(err.Error(), "--no-such-flag") {
		t.Fatalf("error does not mention invalid flag (got '%v')", err)
	}
}

func TestRCloneProviders(t *testing.T) {
	t.Parallel()
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

	if len(rcloneArgs)+len(embeddedConfig) == 0 {
		t.Skipf("Either KOPIA_RCLONE_EMBEDDED_CONFIG_B64 or KOPIA_RCLONE_CONFIG_FILE must be provided")
	}

	rcloneExe := mustGetRcloneExeOrSkip(t)

	for name, rp := range rcloneExternalProviders {
		rp := rp

		opt := &rclone.Options{
			RemotePath:     rp,
			RCloneExe:      rcloneExe,
			RCloneArgs:     rcloneArgs,
			EmbeddedConfig: embeddedConfig,
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := testlogging.Context(t)

			cleanupOldData(ctx, t, opt, defaultCleanupAge)

			// we are using shared storage, append a guid so that tests don't collide
			opt.RemotePath += "/" + uuid.NewString()

			st, err := rclone.New(ctx, opt)
			if err != nil {
				t.Fatalf("unable to connect to rclone backend: %v", err)
			}

			defer st.Close(ctx)

			// at the end of a test delete all blobs that were created.
			defer cleanupAllBlobs(ctx, t, st, 0)

			blobtesting.VerifyStorage(ctx, t, logging.NewWrapper(st, t.Logf, "[RCLONE-STORAGE] "))
			blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
		})
	}
}

func cleanupOldData(ctx context.Context, t *testing.T, opt *rclone.Options, cleanupAge time.Duration) {
	t.Helper()

	t.Logf("cleaning up %v", opt.RemotePath)
	defer t.Logf("finished cleaning up %v", opt.RemotePath)

	// cleanup old data from the bucket
	st, err := rclone.New(ctx, opt)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer st.Close(ctx)

	cleanupAllBlobs(ctx, t, st, cleanupAge)
}

func cleanupAllBlobs(ctx context.Context, t *testing.T, st blob.Storage, cleanupAge time.Duration) {
	t.Helper()

	_ = st.ListBlobs(ctx, "", func(it blob.Metadata) error {
		age := clock.Since(it.Timestamp)
		if age > cleanupAge {
			if err := st.DeleteBlob(ctx, it.BlobID); err != nil {
				t.Errorf("warning: unable to delete %q: %v", it.BlobID, err)
			}
		}
		return nil
	})
}
