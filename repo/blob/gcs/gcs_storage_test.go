package gcs_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

func TestGCSStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := os.Getenv("KOPIA_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("KOPIA_GCS_TEST_BUCKET not provided")
	}

	credDataGZ, err := base64.StdEncoding.DecodeString(os.Getenv("KOPIA_GCS_CREDENTIALS_JSON_GZIP"))
	if err != nil {
		t.Skip("skipping test because GCS credentials file can't be decoded")
	}

	credData, err := gunzip(credDataGZ)
	if err != nil {
		t.Skip("skipping test because GCS credentials file can't be unzipped")
	}

	ctx := testlogging.Context(t)

	st, err := gcs.New(ctx, &gcs.Options{
		BucketName:                   bucket,
		ServiceAccountCredentialJSON: credData,
	})
	if err != nil {
		t.Fatalf("unable to connect to GCS: %v", err)
	}

	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear GCS bucket: %v", err)
	}

	blobtesting.VerifyStorage(ctx, t, st)
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	// delete everything again
	if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}); err != nil {
		t.Fatalf("unable to clear GCS bucket: %v", err)
	}

	if err := st.Close(ctx); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestGCSStorageInvalid(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := os.Getenv("KOPIA_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("KOPIA_GCS_TEST_BUCKET not provided")
	}

	ctx := testlogging.Context(t)

	if _, err := gcs.New(ctx, &gcs.Options{
		BucketName:                    bucket + "-no-such-bucket",
		ServiceAccountCredentialsFile: os.Getenv("KOPIA_GCS_CREDENTIALS_FILE"),
	}); err == nil {
		t.Fatalf("unexpected success connecting to GCS, wanted error")
	}
}

func gunzip(d []byte) ([]byte, error) {
	z, err := gzip.NewReader(bytes.NewReader(d))
	if err != nil {
		return nil, err
	}

	defer z.Close()

	return ioutil.ReadAll(z)
}
