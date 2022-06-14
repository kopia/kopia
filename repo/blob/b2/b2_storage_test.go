package b2_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
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
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Skipf("%s not provided", name)
	}

	return value
}

func TestCleanupOldData(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)

	opt := &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	}

	ctx := testlogging.Context(t)
	st, err := b2.New(ctx, opt)
	require.NoError(t, err)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestB2Storage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)

	opt := &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
		Prefix:     uuid.NewString(),
	}

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	st, err := b2.New(newctx, opt)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestB2StorageInvalidBlob(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)

	ctx := context.Background()

	st, err := b2.New(ctx, &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	})
	require.NoError(t, err)

	defer st.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = st.GetBlob(ctx, blob.ID(fmt.Sprintf("invalid-blob-%v", clock.Now().UnixNano())), 0, 30, &tmp)
	if err == nil {
		t.Errorf("unexpected success when requesting non-existing blob")
	}
}

func TestB2StorageInvalidBucket(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := fmt.Sprintf("invalid-bucket-%v", clock.Now().UnixNano())
	keyID := getEnvOrSkip(t, testKeyIDEnv)
	key := getEnvOrSkip(t, testKeyEnv)

	ctx := context.Background()
	_, err := b2.New(ctx, &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	})
	require.Error(t, err)
}

func TestB2StorageInvalidCreds(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := getEnvOrSkip(t, testBucketEnv)
	keyID := "invalid-key-id"
	key := "invalid-key"

	ctx := context.Background()
	_, err := b2.New(ctx, &b2.Options{
		BucketName: bucket,
		KeyID:      keyID,
		Key:        key,
	})
	require.Error(t, err)
}
