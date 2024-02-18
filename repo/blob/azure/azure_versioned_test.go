package azure_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/azure"
	"github.com/kopia/kopia/repo/format"
)

func TestGetBlobVersionsFailsWhenVersioningDisabled(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Versioning disabled
	container := getEnvOrSkip(t, testContainerEnv)
	storageAccount := getEnvOrSkip(t, testStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testStorageKeyEnv)

	ctx := testlogging.Context(t)
	data := make([]byte, 8)
	rand.Read(data)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	opts := &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         prefix,
	}
	st, err := azure.New(newctx, opts, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	// required for PIT versioning check
	err = st.PutBlob(ctx, format.KopiaRepositoryBlobID, gather.FromSlice([]byte(nil)), blob.PutOptions{})
	require.NoError(t, err)

	pit := clock.Now()
	opts.PointInTime = &pit
	_, err = azure.New(ctx, opts, false)
	require.Error(t, err)
}

func TestGetBlobVersions(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Immutable Storage with Versioning enabled
	container := getEnvOrSkip(t, testImmutableContainerEnv)
	storageAccount := getEnvOrSkip(t, testImmutableStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testImmutableStorageKeyEnv)

	createContainer(t, container, storageAccount, storageKey)

	ctx := testlogging.Context(t)
	data := make([]byte, 8)
	rand.Read(data)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	opts := &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         prefix,
	}
	st, err := azure.New(newctx, opts, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	// required for PIT versioning check
	err = st.PutBlob(ctx, format.KopiaRepositoryBlobID, gather.FromSlice([]byte(nil)), blob.PutOptions{})
	require.NoError(t, err)
	err = st.DeleteBlob(ctx, format.KopiaRepositoryBlobID) // blob can be deleted and still work
	require.NoError(t, err)

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
		fmt.Printf("Running test: %s\n", tt.testName)
		opts.PointInTime = tt.pointInTime
		st, err = azure.New(ctx, opts, false)
		require.NoError(t, err)

		var tmp gather.WriteBuffer
		err = st.GetBlob(ctx, blobID, 0, -1, &tmp)
		require.ErrorIs(t, err, tt.expectedError)
		require.Equal(t, tt.expectedBlobData, string(tmp.ToByteSlice()))
	}
}

func TestGetBlobVersionsWithDeletion(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	// must be with Immutable Storage with Versioning enabled
	container := getEnvOrSkip(t, testImmutableContainerEnv)
	storageAccount := getEnvOrSkip(t, testImmutableStorageAccountEnv)
	storageKey := getEnvOrSkip(t, testImmutableStorageKeyEnv)

	createContainer(t, container, storageAccount, storageKey)

	ctx := testlogging.Context(t)
	data := make([]byte, 8)
	rand.Read(data)
	// use context that gets canceled after opening storage to ensure it's not used beyond New().
	newctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	prefix := fmt.Sprintf("test-%v-%x/", clock.Now().Unix(), data)
	opts := &azure.Options{
		Container:      container,
		StorageAccount: storageAccount,
		StorageKey:     storageKey,
		Prefix:         prefix,
	}
	st, err := azure.New(newctx, opts, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		st.Close(ctx)
	})

	// required for PIT versioning check
	err = st.PutBlob(ctx, format.KopiaRepositoryBlobID, gather.FromSlice([]byte(nil)), blob.PutOptions{})
	require.NoError(t, err)

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

	// blob no longer found
	count = getBlobCount(ctx, t, st, blobID)
	require.Equal(t, 0, count)

	opts.PointInTime = &dataTimestamps[1]
	st, err = azure.New(ctx, opts, false)
	require.NoError(t, err)

	// blob visible again with PIT set.
	count = getBlobCount(ctx, t, st, blobID)
	require.Equal(t, 1, count)

	var tmp gather.WriteBuffer
	err = st.GetBlob(ctx, blobID, 0, -1, &tmp)
	require.NoError(t, err)
	require.Equal(t, updatedData, string(tmp.ToByteSlice()))

	opts.PointInTime = &dataTimestamps[0]
	st, err = azure.New(ctx, opts, false)
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
		// sleep because granularity is 1 second and we should separate to show PIT views.
		time.Sleep(1 * time.Second)
	}

	return putTimes, nil
}
