package cache_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

func TestPersistentLRUCache(t *testing.T) {
	cacheDir := testutil.TempDirectory(t)
	ctx := testlogging.Context(t)

	const maxSizeBytes = 1000

	cs, err := cache.NewStorageOrNil(ctx, cacheDir, maxSizeBytes, "subdir")
	if err != nil {
		t.Fatal(err)
	}

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, cache.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
		SweepFrequency: cache.DefaultSweepFrequency,
	})
	if err != nil {
		t.Fatal(err)
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if got := pc.Get(ctx, "key", 0, -1, &tmp); got {
		t.Fatalf("unexpected cache hit on empty cache")
	}

	someData := bytes.Repeat([]byte{1}, 300)

	pc.Put(ctx, "key1", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key1")

	// sleep between adding key1 and the rest to make it easily the oldest
	// even if the filesystem is not very precise keeping time.
	time.Sleep(2 * time.Second)
	pc.Put(ctx, "key2", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key2")
	pc.Put(ctx, "key3", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key3")
	pc.Put(ctx, "key4", gather.FromSlice(someData))
	verifyBlobExists(ctx, t, cs, "key4")

	require.True(t, pc.Get(ctx, "key2", 0, -1, &tmp))

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

	pc, err = cache.NewPersistentCache(ctx, "testing", cs, cache.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
		SweepFrequency: cache.DefaultSweepFrequency,
	})
	if err != nil {
		t.Fatal(err)
	}

	verifyCached(ctx, t, pc, "key1", nil)
	verifyCached(ctx, t, pc, "key2", someData)
	verifyCached(ctx, t, pc, "key3", someData)
	verifyCached(ctx, t, pc, "key4", someData)

	// create another persistent cache based on the same storage but wrong protection key.
	// all reads from cache will be invalid, which means GetOrLoad will fetch them from the source.
	pc2, err := cache.NewPersistentCache(ctx, "testing", cs, cache.ChecksumProtection([]byte{3, 2, 1}), cache.SweepSettings{
		MaxSizeBytes:   maxSizeBytes,
		TouchThreshold: cache.DefaultTouchThreshold,
		SweepFrequency: cache.DefaultSweepFrequency,
	})
	if err != nil {
		t.Fatal(err)
	}

	var tmp2 gather.WriteBuffer
	defer tmp2.Close()

	require.NoError(t, pc2.GetOrLoad(ctx, "key2", func(output *gather.WriteBuffer) error {
		output.Append([]byte{1, 2, 3})
		return nil
	}, &tmp2))

	// make sure we received data returned by the callback.
	require.Equal(t, []byte{1, 2, 3}, tmp2.ToByteSlice())

	// at this point 'cs' was updated with a different checksum, so attempting to read it using
	// 'pc' will return cache miss.
	verifyCached(ctx, t, pc, "key2", nil)
}

func verifyCached(ctx context.Context, t *testing.T, pc *cache.PersistentCache, key string, want []byte) {
	t.Helper()

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if want == nil {
		require.False(t, pc.Get(ctx, key, 0, -1, &tmp))
	} else {
		require.True(t, pc.Get(ctx, key, 0, -1, &tmp))

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
