package gcs_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/tests/testlease"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

const (
	testLeaseID       = "gcs-test"
	testLeaseDuration = 10 * time.Minute
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

	testlease.RunWithLease(t, testLeaseID, testLeaseDuration, func() {
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
	})
}

func TestGCSStorageInvalid(t *testing.T) {
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
