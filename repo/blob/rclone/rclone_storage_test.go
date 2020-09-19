package rclone_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/rclone"
)

func TestRCloneStorage(t *testing.T) {
	ctx := testlogging.Context(t)

	if err := exec.Command("rclone", "version").Run(); err != nil {
		t.Skip("rclone not installed")
	}

	// directory where rclone will store files
	dataDir, err := ioutil.TempDir("", "rclonetest")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dataDir)

	st, err := rclone.New(ctx, &rclone.Options{
		// pass local file as remote path.
		RemotePath: dataDir,
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
