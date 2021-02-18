package cache_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cache"
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

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, cache.ChecksumProtection([]byte{1, 2, 3}), maxSizeBytes, cache.DefaultTouchThreshold, cache.DefaultSweepFrequency)
	if err != nil {
		t.Fatal(err)
	}

	if got := pc.Get(ctx, "key", 0, -1); got != nil {
		t.Fatalf("unexpected cache hit on empty cache: %x", got)
	}

	someData := bytes.Repeat([]byte{1}, 300)

	pc.Put(ctx, "key1", someData)
	verifyBlobExists(ctx, t, cs, "key1")

	// sleep between adding key1 and the rest to make it easily the oldest
	// even if the filesystem is not very precise keeping time.
	time.Sleep(2 * time.Second)
	pc.Put(ctx, "key2", someData)
	verifyBlobExists(ctx, t, cs, "key2")
	pc.Put(ctx, "key3", someData)
	verifyBlobExists(ctx, t, cs, "key3")
	pc.Put(ctx, "key4", someData)
	verifyBlobExists(ctx, t, cs, "key4")

	if got, want := pc.Get(ctx, "key2", 0, -1), someData; !bytes.Equal(got, want) {
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

	pc, err = cache.NewPersistentCache(ctx, "testing", cs, cache.ChecksumProtection([]byte{1, 2, 3}), maxSizeBytes, cache.DefaultTouchThreshold, cache.DefaultSweepFrequency)
	if err != nil {
		t.Fatal(err)
	}

	verifyCached(ctx, t, pc, "key1", nil)
	verifyCached(ctx, t, pc, "key2", someData)
	verifyCached(ctx, t, pc, "key3", someData)
	verifyCached(ctx, t, pc, "key4", someData)
}

func verifyCached(ctx context.Context, t *testing.T, pc *cache.PersistentCache, key string, want []byte) {
	t.Helper()

	if got := pc.Get(ctx, key, 0, -1); !bytes.Equal(got, want) {
		t.Fatalf("invalid cached result for %v: %x, want %x", key, got, want)
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
