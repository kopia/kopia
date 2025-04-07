package gcs_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	gcsclient "cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/gcs"
)

type bucketOpts struct {
	bucket          string
	credentialsJSON []byte
	projectID       string
	isLockedBucket  bool
}

func TestGetBlobVersionsFailsWhenVersioningDisabled(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Versioning disabled.
	bucket := getEnvVarOrSkip(t, testBucketEnv)

	ctx := testlogging.Context(t)
	data := make([]byte, 8)
	rand.Read(data)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	opts := &gcs.Options{
		BucketName:                   bucket,
		ServiceAccountCredentialJSON: getCredJSONFromEnv(t),
		Prefix:                       prefix,
	}
	st, err := gcs.New(newctx, opts, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	pit := clock.Now()
	opts.PointInTime = &pit
	_, err = gcs.New(ctx, opts, false)
	require.Error(t, err)
}

func TestGetBlobVersions(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Versioning enabled.
	bOpts := bucketOpts{
		projectID:       getEnvVarOrSkip(t, testBucketProjectID),
		bucket:          getImmutableBucketNameOrSkip(t),
		credentialsJSON: getCredJSONFromEnv(t),
		isLockedBucket:  true,
	}

	createBucket(t, bOpts)
	validateBucket(t, bOpts)

	ctx := testlogging.Context(t)
	data := make([]byte, 8)
	rand.Read(data)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	opts := &gcs.Options{
		BucketName:                   bOpts.bucket,
		ServiceAccountCredentialJSON: bOpts.credentialsJSON,
		Prefix:                       prefix,
	}
	st, err := gcs.New(newctx, opts, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	const (
		originalData = "original"
		updatedData  = "some update"
		latestData   = "latest version"
	)

	dataBlobs := []string{originalData, updatedData, latestData}

	const blobName = "TestGetBlobVersions"
	blobID := blob.ID(blobName)
	dataTimestamps, err := putBlobs(ctx, st, blobID, dataBlobs)
	require.NoError(t, err)

	pastPIT := dataTimestamps[0].Add(-1 * time.Second)
	futurePIT := dataTimestamps[2].Add(1 * time.Second)

	for _, tt := range []struct {
		testName         string
		pointInTime      *time.Time
		expectedBlobData string
		expectedError    error
	}{
		{
			testName:         "unset PIT",
			pointInTime:      nil,
			expectedBlobData: latestData,
			expectedError:    nil,
		},
		{
			testName:         "set in the future",
			pointInTime:      &futurePIT,
			expectedBlobData: latestData,
			expectedError:    nil,
		},
		{
			testName:         "set in the past",
			pointInTime:      &pastPIT,
			expectedBlobData: "",
			expectedError:    blob.ErrBlobNotFound,
		},
		{
			testName:         "original data",
			pointInTime:      &dataTimestamps[0],
			expectedBlobData: originalData,
			expectedError:    nil,
		},
		{
			testName:         "updated data",
			pointInTime:      &dataTimestamps[1],
			expectedBlobData: updatedData,
			expectedError:    nil,
		},
		{
			testName:         "latest data",
			pointInTime:      &dataTimestamps[2],
			expectedBlobData: latestData,
			expectedError:    nil,
		},
	} {
		t.Run(tt.testName, func(t *testing.T) {
			opts.PointInTime = tt.pointInTime
			st, err = gcs.New(ctx, opts, false)
			require.NoError(t, err)

			var tmp gather.WriteBuffer
			err = st.GetBlob(ctx, blobID, 0, -1, &tmp)
			require.ErrorIs(t, err, tt.expectedError)
			require.Equal(t, tt.expectedBlobData, string(tmp.ToByteSlice()))
		})
	}
}

func TestGetBlobVersionsWithDeletion(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Versioning enabled.
	bOpts := bucketOpts{
		projectID:       getEnvVarOrSkip(t, testBucketProjectID),
		bucket:          getImmutableBucketNameOrSkip(t),
		credentialsJSON: getCredJSONFromEnv(t),
		isLockedBucket:  true,
	}

	createBucket(t, bOpts)
	validateBucket(t, bOpts)

	ctx := testlogging.Context(t)
	data := make([]byte, 8)
	rand.Read(data)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	opts := &gcs.Options{
		BucketName:                   bOpts.bucket,
		ServiceAccountCredentialJSON: bOpts.credentialsJSON,
		Prefix:                       prefix,
	}
	st, err := gcs.New(newctx, opts, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	const (
		originalData = "original"
		updatedData  = "some update"
	)

	dataBlobs := []string{originalData, updatedData}

	const blobName = "TestGetBlobVersionsWithDeletion"
	blobID := blob.ID(blobName)
	dataTimestamps, err := putBlobs(ctx, st, blobID, dataBlobs)
	require.NoError(t, err)

	count := getBlobCount(ctx, t, st, blobID)
	require.Equal(t, 1, count)

	err = st.DeleteBlob(ctx, blobID)
	require.NoError(t, err)

	// blob no longer found.
	count = getBlobCount(ctx, t, st, blobID)
	require.Equal(t, 0, count)

	opts.PointInTime = &dataTimestamps[1]
	st, err = gcs.New(ctx, opts, false)
	require.NoError(t, err)

	// blob visible again with PIT set.
	count = getBlobCount(ctx, t, st, blobID)
	require.Equal(t, 1, count)

	var tmp gather.WriteBuffer
	err = st.GetBlob(ctx, blobID, 0, -1, &tmp)
	require.NoError(t, err)
	require.Equal(t, updatedData, string(tmp.ToByteSlice()))

	opts.PointInTime = &dataTimestamps[0]
	st, err = gcs.New(ctx, opts, false)
	require.NoError(t, err)

	err = st.GetBlob(ctx, blobID, 0, -1, &tmp)
	require.NoError(t, err)
	require.Equal(t, originalData, string(tmp.ToByteSlice()))
}

func putBlobs(ctx context.Context, cli blob.Storage, blobID blob.ID, blobs []string) ([]time.Time, error) {
	var putTimes []time.Time

	for _, b := range blobs {
		if err := cli.PutBlob(ctx, blobID, gather.FromSlice([]byte(b)), blob.PutOptions{}); err != nil {
			return nil, errors.Wrap(err, "putting blob")
		}

		m, err := cli.GetMetadata(ctx, blobID)
		if err != nil {
			return nil, errors.Wrap(err, "getting metadata")
		}

		putTimes = append(putTimes, m.Timestamp)
	}

	return putTimes, nil
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

func getImmutableBucketNameOrSkip(t *testing.T) string {
	t.Helper()

	return getEnvVarOrSkip(t, testImmutableBucketEnv)
}
