package gcs_test

import (
	"context"
	"os"
	"testing"

	"github.com/kopia/repo/internal/storagetesting"

	"github.com/kopia/repo/storage"
	"github.com/kopia/repo/storage/gcs"
)

func TestGCSStorage(t *testing.T) {
	bucket := os.Getenv("KOPIA_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("KOPIA_GCS_TEST_BUCKET not provided")
	}

	credsFile := os.Getenv("KOPIA_GCS_CREDENTIALS_FILE")
	if _, err := os.Stat(credsFile); err != nil {
		t.Skip("skipping test because GCS credentials file can't be opened")
	}

	ctx := context.Background()
	st, err := gcs.New(ctx, &gcs.Options{
		BucketName:                bucket,
		ServiceAccountCredentials: credsFile,
	})

	if err != nil {
		t.Fatalf("unable to connect to GCS: %v", err)
	}

	if err := st.ListBlocks(ctx, "", func(bm storage.BlockMetadata) error {
		return st.DeleteBlock(ctx, bm.BlockID)
	}); err != nil {
		t.Fatalf("unable to clear GCS bucket: %v", err)
	}

	storagetesting.VerifyStorage(ctx, t, st)
	storagetesting.AssertConnectionInfoRoundTrips(ctx, t, st)

	// delete everything again
	if err := st.ListBlocks(ctx, "", func(bm storage.BlockMetadata) error {
		return st.DeleteBlock(ctx, bm.BlockID)
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
		BucketName:                bucket + "-no-such-bucket",
		ServiceAccountCredentials: os.Getenv("KOPIA_GCS_CREDENTIALS_FILE"),
	})

	if err != nil {
		t.Fatalf("unable to connect to GCS: %v", err)
	}

	defer st.Close(ctx)
	if err := st.PutBlock(ctx, "xxx", []byte{1, 2, 3}); err == nil {
		t.Errorf("unexpecte success when adding to non-existent bucket")
	}
}
