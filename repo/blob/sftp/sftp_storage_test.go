package sftp_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

func TestSFTPStorageValid(t *testing.T) {
	for _, embedCreds := range []bool{false, true} {
		embedCreds := embedCreds
		t.Run(fmt.Sprintf("Embed=%v", embedCreds), func(t *testing.T) {
			ctx := testlogging.Context(t)

			st, err := createSFTPStorage(ctx, t, embedCreds)
			if err != nil {
				t.Fatalf("unable to connect to SSH: %v", err)
			}

			deleteBlobs(ctx, t, st)

			blobtesting.VerifyStorage(ctx, t, st)
			blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

			// delete everything again
			deleteBlobs(ctx, t, st)

			if err := st.Close(ctx); err != nil {
				t.Fatalf("err: %v", err)
			}
		})
	}
}

func deleteBlobs(ctx context.Context, t *testing.T, st blob.Storage) {
	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear sftp storage: %v", err)
	}
}

func createSFTPStorage(ctx context.Context, t *testing.T, embed bool) (blob.Storage, error) {
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

	knownHostsFile := os.Getenv("KOPIA_SFTP_KNOWN_HOSTS_FILE")
	if _, err = os.Stat(knownHostsFile); err != nil {
		t.Skip("skipping test because SFTP known hosts file can't be opened")
	}

	opt := &sftp.Options{
		Path:           path,
		Host:           host,
		Username:       usr,
		Port:           int(port),
		Keyfile:        keyfile,
		KnownHostsFile: knownHostsFile,
	}

	if embed {
		opt.KeyData = mustReadFileToString(t, opt.Keyfile)
		opt.Keyfile = ""

		opt.KnownHostsData = mustReadFileToString(t, opt.KnownHostsFile)
		opt.KnownHostsFile = ""
	}

	return sftp.New(ctx, opt)
}

func mustReadFileToString(t *testing.T, fname string) string {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}
