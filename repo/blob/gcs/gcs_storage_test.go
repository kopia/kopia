package gcs_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"os"
	"strings"
	"testing"

	gcsclient "cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

const (
	testBucketEnv                 = "KOPIA_GCS_TEST_BUCKET"
	testBucketProjectID           = "KOPIA_GCS_TEST_PROJECT_ID"
	testBucketCredentialsJSONGzip = "KOPIA_GCS_CREDENTIALS_JSON_GZIP"
	testImmutableBucketEnv        = "KOPIA_GCS_TEST_IMMUTABLE_BUCKET"
)

type bucketOpts struct {
	bucket          string
	credentialsJSON []byte
	projectID       string
	isLockedBucket  bool
}

func createBucket(t *testing.T, opts bucketOpts) {
	t.Helper()
	ctx := context.Background()

	cli, err := gcsclient.NewClient(ctx, option.WithCredentialsJSON(opts.credentialsJSON))
	require.NoError(t, err, "unable to create GCS client")

	attrs := &gcsclient.BucketAttrs{}

	bucketHandle := cli.Bucket(opts.bucket)
	if opts.isLockedBucket {
		attrs.VersioningEnabled = true
		bucketHandle = bucketHandle.SetObjectRetention(true)
	}

	err = bucketHandle.Create(ctx, opts.projectID, attrs)
	if err == nil {
		return
	}

	if strings.Contains(err.Error(), "The requested bucket name is not available") {
		return
	}

	if strings.Contains(err.Error(), "Your previous request to create the named bucket succeeded and you already own it") {
		return
	}

	t.Fatalf("issue creating bucket: %v", err)
}

func validateBucket(t *testing.T, opts bucketOpts) {
	t.Helper()
	ctx := context.Background()

	cli, err := gcsclient.NewClient(ctx, option.WithCredentialsJSON(opts.credentialsJSON))
	require.NoError(t, err, "unable to create GCS client")

	attrs, err := cli.Bucket(opts.bucket).Attrs(ctx)
	require.NoError(t, err)

	if opts.isLockedBucket {
		require.True(t, attrs.VersioningEnabled)
		require.Equal(t, "Enabled", attrs.ObjectRetentionMode)
	}
}

func TestCleanupOldData(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)
	ctx := testlogging.Context(t)

	st, err := gcs.New(ctx, mustGetOptionsOrSkip(t, ""), false)
	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestGCSStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	st, err := gcs.New(newctx, mustGetOptionsOrSkip(t, uuid.NewString()), false)

	cancel()
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	blobtesting.VerifyStorage(ctx, t, st, blob.PutOptions{})

	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, blobtesting.TestValidationOptions))
}

func TestGCSStorageInvalid(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	bucket := getEnvVarOrSkip(t, testBucketEnv)

	ctx := testlogging.Context(t)

	_, err := gcs.New(ctx, &gcs.Options{
		BucketName:                   bucket + "-no-such-bucket",
		ServiceAccountCredentialJSON: getCredJSONFromEnv(t),
	}, false)
	require.Error(t, err, "unexpected success connecting to GCS, wanted error")
}

func gunzip(d []byte) ([]byte, error) {
	z, err := gzip.NewReader(bytes.NewReader(d))
	if err != nil {
		return nil, err
	}

	defer z.Close()

	return io.ReadAll(z)
}

func getEnvVarOrSkip(t *testing.T, envVarName string) string {
	t.Helper()

	v := os.Getenv(envVarName)
	if v == "" {
		t.Skipf("%q is not set", envVarName)
	}

	return v
}

func getCredJSONFromEnv(t *testing.T) []byte {
	t.Helper()

	b64Data := getEnvVarOrSkip(t, testBucketCredentialsJSONGzip)

	credDataGZ, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		t.Skip("skipping test because GCS credentials file can't be decoded")
	}

	credJSON, err := gunzip(credDataGZ)
	if err != nil {
		t.Skip("skipping test because GCS credentials file can't be unzipped")
	}

	return credJSON
}

func mustGetOptionsOrSkip(t *testing.T, prefix string) *gcs.Options {
	t.Helper()

	bucket := getEnvVarOrSkip(t, testBucketEnv)

	return &gcs.Options{
		BucketName:                   bucket,
		ServiceAccountCredentialJSON: getCredJSONFromEnv(t),
		Prefix:                       prefix,
	}
}

func getBlobCount(ctx context.Context, t *testing.T, st blob.Storage, prefix blob.ID) int {
	t.Helper()

	var count int

	err := st.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		count++
		return nil
	})
	require.NoError(t, err)

	return count
}
