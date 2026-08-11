//go:build !no_extra_providers

package storj_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/storj"
)

func getEnvOrSkip(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		t.Skipf("%s not provided", name)
	}

	return value
}

func TestStorjStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	accessgrant := getEnvOrSkip(t, "STORJ_ACCESS_GRANT")

	opt := &storj.Options{
		BucketName:  getEnvOrSkip(t, "STORJ_TEST_BUCKET"),
		AccessGrant: accessgrant,
		Prefix:      uuid.NewString(),
	}

	ctx := testlogging.Context(t)

	st, err := storj.New(ctx, opt, false)
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestCleanupOldData(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	opt := &storj.Options{
		BucketName:  getEnvOrSkip(t, "STORJ_TEST_BUCKET"),
		AccessGrant: getEnvOrSkip(t, "STORJ_ACCESS_GRANT"),
	}

	ctx := testlogging.Context(t)

	st, err := storj.New(ctx, opt, false)
	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestStorjStorageInvalidBucket(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	accessgrant := getEnvOrSkip(t, "STORJ_ACCESS_GRANT")

	ctx := context.Background()

	// Unlike b2/GCS, storj.New() calls Options.BucketName through
	// EnsureBucket, which creates the bucket if it doesn't already exist,
	// so a merely-nonexistent-but-well-formed bucket name would succeed
	// rather than error. An empty bucket name is rejected server-side
	// regardless of whether it pre-exists.
	_, err := storj.New(ctx, &storj.Options{
		BucketName:  "",
		AccessGrant: accessgrant,
	}, false)
	require.Error(t, err)
}

func TestStorjStorageInvalidCreds(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := getEnvOrSkip(t, "STORJ_TEST_BUCKET")

	ctx := context.Background()

	_, err := storj.New(ctx, &storj.Options{
		BucketName:  bucket,
		AccessGrant: "invalid-access-grant",
	}, false)
	require.Error(t, err)
}
