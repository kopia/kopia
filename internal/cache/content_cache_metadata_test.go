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

func TestContentCacheForMetadata(t *testing.T) {
	ctx := testlogging.Context(t)

	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)

	cacheData := blobtesting.DataMap{}
	metadataCacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil).(cache.Storage)

	metadataCache, err := cache.NewContentCacheForMetadata(ctx, underlying, metadataCacheStorage, cache.SweepSettings{
		MaxSizeBytes: 100,
	})
	require.NoError(t, err)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	// get something we don't have in the underlying storage
	require.ErrorIs(t, metadataCache.GetContent(ctx, "key1", "blob1", 0, 3, &tmp), blob.ErrBlobNotFound)

	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))

	require.NoError(t, metadataCache.GetContent(ctx, "key1", "blob1", 0, -1, &tmp))
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, tmp.ToByteSlice())

	require.NoError(t, metadataCache.GetContent(ctx, "key1", "blob1", 0, 3, &tmp))
	require.Equal(t, []byte{1, 2, 3}, tmp.ToByteSlice())

	// cache has the entire blob
	require.Len(t, cacheData, 1)

	require.NoError(t, metadataCache.GetContent(ctx, "key1", "blob1", 3, 3, &tmp))
	require.Equal(t, []byte{4, 5, 6}, tmp.ToByteSlice())

	// out of bounds
	require.Error(t, metadataCache.GetContent(ctx, "key1", "blob1", 3, 9, &tmp))

	// cache has the entire blob
	require.Len(t, cacheData, 1)

	metadataCache.Close(ctx)

	// get slice with cache miss
	require.NoError(t, underlying.PutBlob(ctx, "blob2", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))
	require.NoError(t, metadataCache.GetContent(ctx, "key1", "blob2", 3, 3, &tmp))
	require.Equal(t, []byte{4, 5, 6}, tmp.ToByteSlice())
}

func TestContentCacheForMetadata_Passthrough(t *testing.T) {
	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)

	ctx := testlogging.Context(t)

	metadataCache, err := cache.NewContentCacheForMetadata(ctx, underlying, nil, cache.SweepSettings{
		MaxSizeBytes: 100,
	})

	require.NoError(t, err)
	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.NoError(t, metadataCache.GetContent(ctx, "key1", "blob1", 0, -1, &tmp))
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6}, tmp.ToByteSlice())
}
