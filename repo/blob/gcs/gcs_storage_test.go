package gcs_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

func TestCleanupOldData(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)
	ctx := testlogging.Context(t)

	st, err := gcs.New(ctx, mustGetOptionsOrSkip(t, ""))
	require.NoError(t, err)

	defer st.Close(ctx)

	blobtesting.CleanupOldData(ctx, t, st, blobtesting.MinCleanupAge)
}

func TestGCSStorage(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	st, err := gcs.New(ctx, mustGetOptionsOrSkip(t, uuid.NewString()))
	require.NoError(t, err)

	defer st.Close(ctx)
	defer blobtesting.CleanupOldData(ctx, t, st, 0)

	options := []blob.PutOptions{
		{},
		{DoNotRecreate: true},
	}

	for _, opt := range options {
		blobtesting.VerifyStorage(ctx, t, st, opt)
	}

	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, st)
	validateOpts := blobtesting.TestValidationOptions
	validateOpts.SupportIdempotentCreates = true
	require.NoError(t, providervalidation.ValidateProvider(ctx, st, validateOpts))
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

	return io.ReadAll(z)
}

func mustGetOptionsOrSkip(t *testing.T, prefix string) *gcs.Options {
	t.Helper()

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

	return &gcs.Options{
		BucketName:                   bucket,
		ServiceAccountCredentialJSON: credData,
		Prefix:                       prefix,
	}
}
