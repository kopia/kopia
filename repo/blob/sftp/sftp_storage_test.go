package sftp_test

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

const (
	t1 = "392ee1bc299db9f235e046a62625afb84902"
	t2 = "2a7ff4f29eddbcd4c18fa9e73fec20bbb71f"
	t3 = "0dae5918f83e6a24c8b3e274ca1026e43f24"
)

func TestSFTPStorageValid(t *testing.T) {
	ctx := testlogging.Context(t)

	if runtime.GOOS == "windows" {
		t.Skip("temporarily disabled - https://github.com/kopia/kopia/issues/216")
	}

	st, err := createSFTPStorage(ctx, t)

	if err != nil {
		t.Fatalf("unable to connect to SSH: %v", err)
	}

	assertNoError(t, st.PutBlob(ctx, t1, []byte{1}))
	time.Sleep(1 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution
	assertNoError(t, st.PutBlob(ctx, t2, []byte{1}))
	time.Sleep(1 * time.Second)
	assertNoError(t, st.PutBlob(ctx, t3, []byte{1}))

	deleteBlobs(ctx, t, st)

	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	// delete everything again
	deleteBlobs(ctx, t, st)

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func deleteBlobs(ctx context.Context, t *testing.T, st blob.Storage) {
	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear sftp storage: %v", err)
	}
}

func createSFTPStorage(ctx context.Context, t *testing.T) (blob.Storage, error) {
	host := os.Getenv("KOPIA_SFTP_TEST_HOST")
	if host == "" {
		t.Skip("KOPIA_SFTP_TEST_HOST not provided")
	}

	envPort := os.Getenv("KOPIA_SFTP_TEST_PORT")
	if envPort == "" {
		t.Skip("KOPIA_SFTP_TEST_PORT not provided")
	}

	port, err := strconv.ParseInt(envPort, 10, 64)
	if err != nil {
		t.Skip("skipping test because port is not numeric")
	}

	path := os.Getenv("KOPIA_SFTP_TEST_PATH")
	if path == "" {
		t.Skip("KOPIA_SFTP_TEST_PATH not provided")
	}

	keyfile := os.Getenv("KOPIA_SFTP_KEYFILE")
	if _, err = os.Stat(keyfile); err != nil {
		t.Skip("skipping test because SFTP keyfile can't be opened")
	}

	usr := os.Getenv("KOPIA_SFTP_TEST_USER")
	if usr == "" {
		t.Skip("KOPIA_SFTP_TEST_USER not provided")
	}

	knownHosts := os.Getenv("KOPIA_SFTP_KNOWN_HOSTS_FILE")
	if _, err = os.Stat(knownHosts); err != nil {
		t.Skip("skipping test because SFTP known hosts file can't be opened")
	}

	return sftp.New(ctx, &sftp.Options{
		Path:       path,
		Host:       host,
		Username:   usr,
		Port:       int(port),
		Keyfile:    keyfile,
		KnownHosts: knownHosts,
	})
}
