package r2

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

func TestToS3Options(t *testing.T) {
	t.Parallel()

	t.Run("default endpoint", func(t *testing.T) {
		t.Parallel()

		got, err := (&Options{
			AccountID:       "abc123",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.NoError(t, err)
		require.Equal(t, "abc123.r2.cloudflarestorage.com", got.Endpoint)
		require.Equal(t, "auto", got.Region)
		require.False(t, got.DoNotUseTLS)
	})

	t.Run("eu jurisdiction", func(t *testing.T) {
		t.Parallel()

		got, err := (&Options{
			AccountID:       "abc123",
			Jurisdiction:    "eu",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.NoError(t, err)
		require.Equal(t, "abc123.eu.r2.cloudflarestorage.com", got.Endpoint)
		require.Equal(t, "auto", got.Region)
	})

	t.Run("fedramp jurisdiction", func(t *testing.T) {
		t.Parallel()

		got, err := (&Options{
			AccountID:       "abc123",
			Jurisdiction:    "fedramp",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.NoError(t, err)
		require.Equal(t, "abc123.fedramp.r2.cloudflarestorage.com", got.Endpoint)
		require.Equal(t, "auto", got.Region)
	})

	t.Run("explicit https endpoint", func(t *testing.T) {
		t.Parallel()

		got, err := (&Options{
			Endpoint:        "https://custom.example.com",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.NoError(t, err)
		require.Equal(t, "custom.example.com", got.Endpoint)
		require.False(t, got.DoNotUseTLS)
	})

	t.Run("explicit http endpoint", func(t *testing.T) {
		t.Parallel()

		got, err := (&Options{
			Endpoint:        "http://localhost:9000",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.NoError(t, err)
		require.Equal(t, "localhost:9000", got.Endpoint)
		require.True(t, got.DoNotUseTLS)
	})

	t.Run("host endpoint", func(t *testing.T) {
		t.Parallel()

		got, err := (&Options{
			Endpoint:        "localhost:9000",
			BucketName:      "test-bucket",
			DoNotUseTLS:     true,
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.NoError(t, err)
		require.Equal(t, "localhost:9000", got.Endpoint)
		require.True(t, got.DoNotUseTLS)
	})

	t.Run("unknown jurisdiction", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			AccountID:       "abc123",
			Jurisdiction:    "moon",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, `unsupported R2 jurisdiction: "moon"`)
	})

	t.Run("account required", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, "account ID must be specified")
	})

	t.Run("account must be host safe", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			AccountID:       "abc123/other",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, "account ID must contain only letters and digits")
	})

	t.Run("bucket required", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			AccountID:       "abc123",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, "bucket name must be specified")
	})

	t.Run("path rejected", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			Endpoint:        "https://custom.example.com/path",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, "endpoint must not include path")
	})

	t.Run("host endpoint path rejected", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			Endpoint:        "custom.example.com/path",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, "endpoint must not include path")
	})

	t.Run("empty host rejected", func(t *testing.T) {
		t.Parallel()

		_, err := (&Options{
			Endpoint:        "https://",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		}).toS3Options()

		require.ErrorContains(t, err, "endpoint host must be specified")
	})
}

func TestUnsupportedObjectLock(t *testing.T) {
	t.Parallel()

	st := &r2Storage{
		Storage: blobtesting.NewMapStorage(nil, nil, nil),
		opt: Options{
			AccountID:       "abc123",
			BucketName:      "test-bucket",
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		},
	}

	ctx := context.Background()

	err := st.PutBlob(ctx, "test", gather.FromSlice([]byte("test")), blob.PutOptions{
		RetentionMode:   blob.Governance,
		RetentionPeriod: time.Hour,
	})
	require.True(t, errors.Is(err, blob.ErrUnsupportedPutBlobOption))

	err = st.ExtendBlobRetention(ctx, "test", blob.ExtendOptions{
		RetentionMode:   blob.Governance,
		RetentionPeriod: time.Hour,
	})
	require.ErrorIs(t, err, blob.ErrUnsupportedObjectLock)
}
