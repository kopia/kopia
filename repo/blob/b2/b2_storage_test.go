package b2_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/b2"
)

const (
	testBucketEnv = "KOPIA_B2_TEST_BUCKET"
	testKeyIDEnv  = "KOPIA_B2_TEST_KEY_ID"
	testKeyEnv    = "KOPIA_B2_TEST_KEY"
)

func getEnvOrSkip(t *testing.T, name string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		t.Skip(fmt.Sprintf("%s not provided", name))
	}

	return value
}

func TestB2Storage(t *testing.T) {
	t.Parallel()
	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)
	testutil.Retry(t, func(t *testutil.RetriableT) {

		data := make([]byte, 8)
		rand.Read(data) //nolint:errcheck

		ctx := context.Background()
		st, err := b2.New(ctx, &b2.Options{
			BucketName: bucket,
			KeyID:      keyID,
			Key:        key,
			Prefix:     fmt.Sprintf("test-%v-%x-", time.Now().Unix(), data),
		})

		if err != nil {
			t.Fatalf("unable to build b2 storage: %v", err)
		}

		if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
			return st.DeleteBlob(ctx, bm.BlobID)
		}); err != nil {
			t.Fatalf("unable to clear b2 bucket: %v", err)
		}

		blobtesting.VerifyStorage(ctx, t, st)
		blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)

		// delete everything again
		if err := st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
			return st.DeleteBlob(ctx, bm.BlobID)
		}); err != nil {
			t.Fatalf("unable to clear b2 bucket: %v", err)
		}

		if err := st.Close(ctx); err != nil {
			t.Fatalf("err: %v", err)
		}
	})
}

func TestB2StorageInvalidBlob(t *testing.T) {
	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)

	ctx := context.Background()
	st, err := b2.New(ctx, &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	})

	if err != nil {
		t.Fatalf("unable to build b2 storage: %v", err)
	}

	defer st.Close(ctx)

	_, err = st.GetBlob(ctx, blob.ID(fmt.Sprintf("invalid-blob-%v", time.Now().UnixNano())), 0, 30)
	if err == nil {
		t.Errorf("unexpected success when requesting non-existing blob")
	}
}

func TestB2StorageInvalidBucket(t *testing.T) {
	bucket := fmt.Sprintf("invalid-bucket-%v", time.Now().UnixNano())
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)

	ctx := context.Background()
	_, err := b2.New(ctx, &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	})

	if err == nil {
		t.Errorf("unexpected success building b2 storage, wanted error")
	}
}

func TestB2StorageInvalidCreds(t *testing.T) {
	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := "invalid-key-id"
	key := "invalid-key"

	ctx := context.Background()
	_, err := b2.New(ctx, &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	})

	if err == nil {
		t.Errorf("unexpected success building b2 storage, wanted error")
	}
}
