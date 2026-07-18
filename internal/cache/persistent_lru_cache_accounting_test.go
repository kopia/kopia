package cache_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

// TestPersistentLRUCache_CacheHitsPreserveHardLimit reproduces #5408: a
// long-running reader (e.g. a `kopia mount` + diff/restore) fills the content
// cache far beyond its configured hard limit, and only a fresh `kopia cache
// info` (which re-scans the directory) brings it back down.
//
// The mechanism: on every full-blob cache hit, getPartialCacheHit records the
// *request* length (-1 for a full read) as the item's tracked size. That
// corrupts the running totalDataBytes downward, so both the soft and the hard
// sweep limits stop firing and the on-disk cache grows without bound until the
// next initialScan recomputes the size from the actual blobs.
func TestPersistentLRUCache_CacheHitsPreserveHardLimit(t *testing.T) {
	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	// monotonically increasing clock so every TouchBlob advances the stored
	// mtime and the accounting update path is exercised on each cache hit.
	var (
		mu   sync.Mutex
		base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		tick int64
	)

	timeNow := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		tick++

		return base.Add(time.Duration(tick) * time.Second)
	}

	const (
		limitBytes = 1000
		blobSize   = 300
	)

	data := blobtesting.DataMap{}
	cs := testutil.EnsureType[cache.Storage](t, blobtesting.NewMapStorageWithLimit(data, nil, timeNow, -1))

	pc, err := cache.NewPersistentCache(ctx, "testing", cs, cacheprot.ChecksumProtection([]byte{1, 2, 3}), cache.SweepSettings{
		MaxSizeBytes:   limitBytes,
		LimitBytes:     limitBytes,      // hard limit, enforced regardless of item age
		MinSweepAge:    time.Hour,       // keep every item "young" so ONLY the hard limit can evict
		TouchThreshold: time.Nanosecond, // refresh mtime on every read (the real default is 10m, which a long mount crosses)
	}, nil, timeNow)
	require.NoError(t, err)

	payload := bytes.Repeat([]byte{1}, blobSize)

	// Prime the cache up to its hard limit (3 * ~332 bytes <= 1000).
	pc.Put(ctx, "key1", gather.FromSlice(payload))
	pc.Put(ctx, "key2", gather.FromSlice(payload))
	pc.Put(ctx, "key3", gather.FromSlice(payload))

	// Simulate a sustained read-heavy mount: each cached blob is read back
	// (a full-blob cache hit) while new content keeps being fetched. Every hit
	// records -1 (the request length of a full read) as the item's tracked
	// size, so the running total drifts ever more negative and the sweep never
	// sees the cache cross its hard limit -- even though the directory keeps
	// growing on disk.
	const total = 30

	for i := 1; i <= total; i++ {
		var tmp gather.WriteBuffer
		pc.TestingGetFull(ctx, fmt.Sprintf("key%d", i), &tmp)
		tmp.Close()

		pc.Put(ctx, fmt.Sprintf("key%d", i+3), gather.FromSlice(payload))
	}

	// The hard limit must cap the on-disk cache regardless of the reads above.
	// 1000 bytes / ~332 per blob => at most ~3 retained blobs.
	const maxRetained = 5

	require.LessOrEqualf(t, len(data), maxRetained,
		"content cache holds %d blobs, far above what the %d-byte hard limit allows: "+
			"cache-hit size accounting was corrupted and the sweep stopped enforcing the limit",
		len(data), limitBytes)

	// The tracked size must stay consistent with the blobs actually on disk, so
	// that a subsequent Put keeps enforcing the limit without needing a re-scan.
	require.LessOrEqual(t, totalStoredBytes(ctx, t, cs), int64(limitBytes)+blobSize+int64(cacheprot.ChecksumProtection([]byte{1, 2, 3}).OverheadBytes()))
}

func totalStoredBytes(ctx context.Context, t *testing.T, cs cache.Storage) int64 {
	t.Helper()

	var total int64

	require.NoError(t, cs.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		total += bm.Length

		return nil
	}))

	return total
}
