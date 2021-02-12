package rclone_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/rclone"
)

func TestRCloneStorage(t *testing.T) {
	ctx := testlogging.Context(t)

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
	ctx := testlogging.Context(t)

	_, err := rclone.New(ctx, &rclone.Options{
		RCloneExe:  "no-such-rclone",
		RemotePath: "mmm:/tmp/rclonetest",
	})
	if err == nil {
		t.Fatalf("unexpected success wen starting rclone")
	}
}
