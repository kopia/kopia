package cache_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/fault"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

func TestPersistentLRUCache(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	const maxSizeBytes = 1000

	cs := blobtesting.NewMapStorageWithLimit(blobtesting.DataMap{}, nil, nil, maxSizeBytes).(cache.Storage)

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
	}, nil, clock.Now)
	require.NoError(t, err)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if got := pc.GetFull(ctx, "key", &tmp); got {
		t.Fatalf("unexpected cache hit on empty cache")
	}

	someData := bytes.Repeat([]byte{1}, 300)

	pc.Put(ctx, "key1", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key1")

	pc.Put(ctx, "key2", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key2")
	pc.Put(ctx, "key3", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key3")
	pc.Put(ctx, "key4", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key4")

	require.True(t, pc.GetFull(ctx, "key2", &tmp))

	if got, want := tmp.ToByteSlice(), someData; !bytes.Equal(got, want) {
		t.Fatalf("invalid data retrieved from cache: %x", got)
	}

	// final sweep is performed on close at which time key1 becomes candidate
	// for expulsion from cache because it's the oldest and we have 1200 bytes in the cache
	// but the limit is only 1000.
	pc.Close(ctx)

	verifyBlobDoesNotExist(ctx, t, cs, "key1")
	verifyBlobExists(ctx, t, cs, "key2")
	verifyBlobExists(ctx, t, cs, "key3")
	verifyBlobExists(ctx, t, cs, "key4")

	pc, err = cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
	}, nil, clock.Now)
	require.NoError(t, err)

	verifyCached(ctx, t, pc, "key1", nil)
	verifyCached(ctx, t, pc, "key2", someData)
	verifyCached(ctx, t, pc, "key3", someData)
	verifyCached(ctx, t, pc, "key4", someData)

	// create another persistent cache based on the same storage but wrong protection key.
	// all reads from cache will be invalid, which means GetOrLoad will fetch them from the source.
	pc2, err := cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{3, 2, 1}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
	}, nil, clock.Now)
	require.NoError(t, err)

	someError := errors.New("some error")

	var tmp2 gather.WriteBuffer
	defer tmp2.Close()

	require.NoError(t, pc2.GetOrLoad(ctx, "key2", func(output *gather.WriteBuffer) error {
		output.Append([]byte{1, 2, 3})
		return nil
	}, &tmp2))

	require.NoError(t, pc2.GetOrLoad(ctx, "key2", func(output *gather.WriteBuffer) error {
		return someError
	}, &tmp2))

	// make sure we received data returned by the callback.
	require.Equal(t, []byte{1, 2, 3}, tmp2.ToByteSlice())

	// at this point 'cs' was updated with a different checksum, so attempting to read it using
	// 'pc' will return cache miss.
	verifyCached(ctx, t, pc, "key2", nil)

	require.ErrorIs(t, pc2.GetOrLoad(ctx, "key9", func(output *gather.WriteBuffer) error {
		return someError
	}, &tmp2), someError)
}

type faultyCache struct {
	*blobtesting.FaultyStorage
}

func (faultyCache) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) (time.Time, error) {
	return time.Time{}, nil
}

func TestPersistentLRUCache_Invalid(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	someError := errors.New("some error")

	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	fs.AddFault(blobtesting.MethodGetMetadata).ErrorInstead(someError)

	pc, err := cache.NewPersistentCache(ctx, "test", fc, nil, cache.SweepSettings{}, nil, clock.Now)
	require.ErrorIs(t, err, someError)
	require.Nil(t, pc)
}

func TestPersistentLRUCache_GetDeletesInvalidBlob(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	someError := errors.New("some error")

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1000

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{MaxSizeBytes: maxSizeBytes}, nil, clock.Now)
	require.NoError(t, err)

	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))

	// corrupt cached data
	data["key"][0] ^= 1

	var tmp gather.WriteBuffer
	defer tmp.Close()

	// simulate failure when trying to delete.
	fs.AddFault(blobtesting.MethodDeleteBlob).ErrorInstead(someError)

	pc.GetFull(ctx, "key", &tmp)
}

func TestPersistentLRUCache_PutIgnoresStorageFailure(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	someError := errors.New("some error")

	data := blobtesting.DataMap{}

	st := blobtesting.NewMapStorage(data, nil, nil)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{}, nil, clock.Now)
	require.NoError(t, err)

	fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.False(t, pc.GetFull(ctx, "key", &tmp))

	require.Equal(t, 1, fs.NumCalls(blobtesting.MethodPutBlob))
}

func TestPersistentLRUCache_SweepMinSweepAge(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1000

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
		MinSweepAge:  10 * time.Second,
	}, nil, clock.Now)
	require.NoError(t, err)
	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	pc.Put(ctx, "key2", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3}, 10)))
	time.Sleep(1 * time.Second)

	// simulate error during final sweep
	fs.AddFault(blobtesting.MethodListBlobs).ErrorInstead(errors.New("some error"))
	pc.Close(ctx)

	// both keys are retained since we're under min sweep age
	require.Len(t, data, 2)
}

func TestPersistentLRUCache_SweepIgnoresErrors(t *testing.T) {
	t.Parallel()

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1000

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
	}, nil, clock.Now)
	require.NoError(t, err)

	// ignore delete errors forever
	fs.AddFault(blobtesting.MethodDeleteBlob).ErrorInstead(errors.New("some delete error")).Repeat(1e6)

	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	pc.Put(ctx, "key2", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3}, 10)))
	time.Sleep(500 * time.Millisecond)

	// simulate error during sweep
	fs.AddFaults(blobtesting.MethodListBlobs, fault.New().ErrorInstead(errors.New("some error")))

	time.Sleep(500 * time.Millisecond)

	pc.Close(ctx)

	// both keys are retained since we're under min sweep age
	require.Len(t, data, 2)
}

func TestPersistentLRUCache_Sweep1(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	data := blobtesting.DataMap{}

	const maxSizeBytes = 1

	st := blobtesting.NewMapStorageWithLimit(data, nil, nil, maxSizeBytes)
	fs := blobtesting.NewFaultyStorage(st)
	fc := faultyCache{fs}

	pc, err := cache.NewPersistentCache(ctx, "test", fc, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
		MinSweepAge:  0 * time.Second,
	}, nil, clock.Now)
	require.NoError(t, err)
	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))
	pc.Put(ctx, "key", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3}, 1e6)))
	time.Sleep(1 * time.Second)

	// simulate error during final sweep
	fs.AddFaults(blobtesting.MethodListBlobs, fault.New().ErrorInstead(errors.New("some error")))
	pc.Close(ctx)
}

func TestPersistentLRUCacheNil(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	var pc *cache.PersistentCache

	// no-op
	pc.Close(ctx)
	pc.Put(ctx, "key", gather.FromSlice([]byte{1, 2, 3}))

	var tmp gather.WriteBuffer

	require.False(t, pc.GetFull(ctx, "key", &tmp))

	called := false

	dummyError := errors.New("dummy error")

	require.ErrorIs(t, pc.GetOrLoad(ctx, "key", func(output *gather.WriteBuffer) error {
		called = true
		return dummyError
	}, &tmp), dummyError)

	require.True(t, called)
}

func TestPersistentLRUCache_Defaults(t *testing.T) {
	cacheDir := testutil.TempDirectory(t)
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	const maxSizeBytes = 1000

	cs, err := cache.NewStorageOrNil(ctx, cacheDir, maxSizeBytes, "subdir")
	require.NoError(t, err)

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, nil, cache.SweepSettings{
		MaxSizeBytes: maxSizeBytes,
	}, nil, clock.Now)
	require.NoError(t, err)

	defer pc.Close(ctx)

	pc.Put(ctx, "key1", gather.FromSlice([]byte{1, 2, 3}))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	cs.GetBlob(ctx, "key1", 0, -1, &tmp)
	require.Len(t, tmp.ToByteSlice(), 3)
}

func verifyCached(ctx context.Context, t *testing.T, pc *cache.PersistentCache, key string, want []byte) {
	t.Helper()

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if want == nil {
		require.False(t, pc.GetFull(ctx, key, &tmp))
	} else {
		require.True(t, pc.GetFull(ctx, key, &tmp))

		if got := tmp.ToByteSlice(); !bytes.Equal(got, want) {
			t.Fatalf("invalid cached result for %v: %x, want %x", key, got, want)
		}
	}
}

func verifyBlobExists(ctx context.Context, t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	if _, err := st.GetMetadata(ctx, blobID); err != nil {
		t.Fatalf("blob %v error: %v", blobID, err)
	}
}

func verifyBlobDoesNotExist(ctx context.Context, t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	if _, err := st.GetMetadata(ctx, blobID); !errors.Is(err, blob.ErrBlobNotFound) {
		t.Fatalf("unexpected blob %v error: %v", blobID, err)
	}
}
