package cache_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestContentCacheForData(t *testing.T) {
	ctx := testlogging.Context(t)

	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil).(cache.Storage)

	dataCache, err := cache.NewContentCache(ctx, underlying, cache.Options{
		Storage:    cacheStorage,
		HMACSecret: []byte{1, 2, 3, 4},
		Sweep: cache.SweepSettings{
			MaxSizeBytes: 150,
		},
	}, nil)
	require.NoError(t, err)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	// get something we don't have in the underlying storage
	require.ErrorIs(t, dataCache.GetContent(ctx, "key1", "blob1", 0, 3, &tmp), blob.ErrBlobNotFound)

	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))

	require.NoError(t, dataCache.GetContent(ctx, "xkey1", "blob1", 0, 3, &tmp))
	require.Equal(t, []byte{1, 2, 3}, tmp.ToByteSlice())

	require.NoError(t, dataCache.GetContent(ctx, "xkey2", "blob1", 3, 3, &tmp))
	require.Equal(t, []byte{4, 5, 6}, tmp.ToByteSlice())

	// cache has 2 entries, one for each section of the blob.
	require.Len(t, cacheData, 2)

	// keys are mangled so that last character (which is always 0..9 a..f) is the first.
	require.Contains(t, cacheData, blob.ID("key1x"))
	require.Contains(t, cacheData, blob.ID("key2x"))

	dataCache.Close(ctx)

	// get slice with cache miss
	require.NoError(t, underlying.PutBlob(ctx, "blob2", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))
	require.NoError(t, dataCache.GetContent(ctx, "aaa1", "blob2", 3, 3, &tmp))
	require.Equal(t, []byte{4, 5, 6}, tmp.ToByteSlice())

	// even-length content IDs are not mangled
	require.Len(t, cacheData, 3)
	require.Contains(t, cacheData, blob.ID("aaa1"))

	require.NoError(t, underlying.PutBlob(ctx, "pblob3", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))
	dataCache.PrefetchBlob(ctx, "pblob3")
	require.NoError(t, underlying.DeleteBlob(ctx, "pblob3"))

	// make sure blob ID is properly mangled
	require.Contains(t, cacheData, blob.ID("blob3p"))
	require.NoError(t, dataCache.GetContent(ctx, "aaa3", "pblob3", 2, 4, &tmp))
	require.Equal(t, []byte{3, 4, 5, 6}, tmp.ToByteSlice())
}

func TestContentCacheForData_Passthrough(t *testing.T) {
	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)

	ctx := testlogging.Context(t)

	dataCache, err := cache.NewContentCache(ctx, underlying, cache.Options{}, nil)

	require.NoError(t, err)
	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.NoError(t, dataCache.GetContent(ctx, "key1", "blob1", 0, 5, &tmp))
	require.Equal(t, []byte{1, 2, 3, 4, 5}, tmp.ToByteSlice())
}
