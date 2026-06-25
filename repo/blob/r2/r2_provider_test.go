package r2

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

const (
	testAccountIDEnv       = "KOPIA_R2_TEST_ACCOUNT_ID"
	testEndpointEnv        = "KOPIA_R2_TEST_ENDPOINT"
	testAccessKeyIDEnv     = "KOPIA_R2_TEST_ACCESS_KEY_ID"
	testSecretAccessKeyEnv = "KOPIA_R2_TEST_SECRET_ACCESS_KEY"
	testSessionTokenEnv    = "KOPIA_R2_TEST_SESSION_TOKEN"
	testJurisdictionEnv    = "KOPIA_R2_TEST_JURISDICTION"
)

func TestR2StorageCloudflare(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	options := &Options{
		AccountID:       getEnvOrSkip(t, testAccountIDEnv),
		Endpoint:        os.Getenv(testEndpointEnv),
		AccessKeyID:     getEnvOrSkip(t, testAccessKeyIDEnv),
		SecretAccessKey: getEnvOrSkip(t, testSecretAccessKeyEnv),
		SessionToken:    os.Getenv(testSessionTokenEnv),
		Jurisdiction:    os.Getenv(testJurisdictionEnv),
		BucketName:      "kopia-r2-test-" + uuid.NewString(),
	}

	ctx := testlogging.Context(t)
	cli := createClient(t, options)

	require.NoError(t, cli.MakeBucket(ctx, options.BucketName, minio.MakeBucketOptions{
		Region: r2Region,
	}))

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)
		removeAllObjects(ctx, t, cli, options.BucketName)
		require.NoError(t, cli.RemoveBucket(ctx, options.BucketName))
	})

	st, err := New(ctx, options, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx := testlogging.ContextForCleanup(t)
		blobtesting.CleanupOldData(ctx, t, st, 0)
		require.NoError(t, st.Close(ctx))
	})

	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
}

func getEnvOrSkip(tb testing.TB, name string) string {
	tb.Helper()

	value := os.Getenv(name)
	if value == "" {
		tb.Skipf("Environment variable '%s' not provided", name)
	}

	return value
}

func createClient(tb testing.TB, opt *Options) *minio.Client {
	tb.Helper()

	s3Options, err := opt.toS3Options()
	require.NoError(tb, err)

	minioClient, err := minio.New(s3Options.Endpoint,
		&minio.Options{
			Creds:     credentials.NewStaticV4(opt.AccessKeyID, opt.SecretAccessKey, opt.SessionToken),
			Secure:    !s3Options.DoNotUseTLS,
			Region:    r2Region,
			Transport: nil,
		})
	require.NoError(tb, err)

	return minioClient
}

func removeAllObjects(ctx context.Context, tb testing.TB, cli *minio.Client, bucketName string) {
	tb.Helper()

	objectCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectCh)

		for object := range cli.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err != nil {
				tb.Logf("unable to list object for cleanup: %v", object.Err)
				continue
			}

			objectCh <- object
		}
	}()

	for err := range cli.RemoveObjects(ctx, bucketName, objectCh, minio.RemoveObjectsOptions{}) {
		if err.Err != nil {
			tb.Logf("unable to remove object %v during cleanup: %v", err.ObjectName, err.Err)
		}
	}
}
