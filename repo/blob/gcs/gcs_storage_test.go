package gcs_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

func TestGCSStorage(t *testing.T) {
	bucket := os.Getenv("KOPIA_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("KOPIA_GCS_TEST_BUCKET not provided")
	}

	credData, err := ioutil.ReadFile(os.Getenv("KOPIA_GCS_CREDENTIALS_FILE"))
	if err != nil {
		t.Skip("skipping test because GCS credentials file can't be opened")
	}

	ctx := context.Background()
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
	bucket := os.Getenv("KOPIA_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("KOPIA_GCS_TEST_BUCKET not provided")
	}

	ctx := context.Background()
	st, err := gcs.New(ctx, &gcs.Options{
		BucketName:                    bucket + "-no-such-bucket",
		ServiceAccountCredentialsFile: os.Getenv("KOPIA_GCS_CREDENTIALS_FILE"),
	})

	if err != nil {
		t.Fatalf("unable to connect to GCS: %v", err)
	}

	defer st.Close(ctx)

	if err := st.PutBlob(ctx, "xxx", []byte{1, 2, 3}); err == nil {
		t.Errorf("unexpecte success when adding to non-existent bucket")
	}
}
