package rclone_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
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
