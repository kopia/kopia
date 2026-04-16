package storj_test

import (
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

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)

		blobtesting.CleanupOldData(ctx, t, st, 0)
		st.Close(ctx)
	})

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}
