package content

import (
	"container/heap"
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"

	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

const (
	defaultSweepFrequency = 1 * time.Minute
	defaultTouchThreshold = 10 * time.Minute
)

type cacheKey string

type contentCache struct {
	st             blob.Storage
	cacheStorage   blob.Storage
	maxSizeBytes   int64
	hmacSecret     []byte
	sweepFrequency time.Duration
	touchThreshold time.Duration

	mu                 sync.Mutex
	lastTotalSizeBytes int64

	asyncWG sync.WaitGroup
	closed  chan struct{}
}

type contentToucher interface {
	TouchBlob(ctx context.Context, contentID blob.ID, threshold time.Duration) error
}

func adjustCacheKey(cacheKey cacheKey) cacheKey {
	// content IDs with odd length have a single-byte prefix.
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if len(cacheKey)%2 == 1 {
		return cacheKey[1:] + cacheKey[0:1]
	}

	return cacheKey
}

func (c *contentCache) getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error) {
	cacheKey = adjustCacheKey(cacheKey)

	useCache := shouldUseContentCache(ctx) && c.cacheStorage != nil
	if useCache {
		if b := c.readAndVerifyCacheContent(ctx, cacheKey); b != nil {
			stats.Record(ctx,
				metricContentCacheHitCount.M(1),
				metricContentCacheHitBytes.M(int64(len(b))),
			)

			return b, nil
		}
	}

	stats.Record(ctx, metricContentCacheMissCount.M(1))

	b, err := c.st.GetBlob(ctx, blobID, offset, length)
	if err != nil {
		stats.Record(ctx, metricContentCacheMissErrors.M(1))
	} else {
		stats.Record(ctx, metricContentCacheMissBytes.M(int64(len(b))))
	}

	if err == blob.ErrBlobNotFound {
		// not found in underlying storage
		return nil, err
	}

	if err == nil && useCache {
		// do not report cache writes as uploads.
		if puterr := c.cacheStorage.PutBlob(
			blob.WithUploadProgressCallback(ctx, nil),
			blob.ID(cacheKey),
			gather.FromSlice(hmac.Append(b, c.hmacSecret)),
		); puterr != nil {
			stats.Record(ctx, metricContentCacheStoreErrors.M(1))
			log(ctx).Warningf("unable to write cache item %v: %v", cacheKey, puterr)
		}
	}

	return b, err
}

func (c *contentCache) readAndVerifyCacheContent(ctx context.Context, cacheKey cacheKey) []byte {
	b, err := c.cacheStorage.GetBlob(ctx, blob.ID(cacheKey), 0, -1)
	if err == nil {
		b, err = hmac.VerifyAndStrip(b, c.hmacSecret)
		if err == nil {
			if t, ok := c.cacheStorage.(contentToucher); ok {
				t.TouchBlob(ctx, blob.ID(cacheKey), c.touchThreshold) //nolint:errcheck
			}

			// retrieved from cache and HMAC valid
			return b
		}

		// ignore malformed contents
		log(ctx).Warningf("malformed content %v: %v", cacheKey, err)

		return nil
	}

	if err != blob.ErrBlobNotFound {
		log(ctx).Warningf("unable to read cache %v: %v", cacheKey, err)
	}

	return nil
}

func (c *contentCache) close() {
	close(c.closed)
	c.asyncWG.Wait()
}

func (c *contentCache) sweepDirectoryPeriodically(ctx context.Context) {
	defer c.asyncWG.Done()

	for {
		select {
		case <-c.closed:
			return

		case <-time.After(c.sweepFrequency):
			err := c.sweepDirectory(ctx)
			if err != nil {
				log(ctx).Warningf("contentCache sweep failed: %v", err)
			}
		}
	}
}

// A contentMetadataHeap implements heap.Interface and holds blob.Metadata.
type contentMetadataHeap []blob.Metadata

func (h contentMetadataHeap) Len() int { return len(h) }

func (h contentMetadataHeap) Less(i, j int) bool {
	return h[i].Timestamp.Before(h[j].Timestamp)
}

func (h contentMetadataHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *contentMetadataHeap) Push(x interface{}) {
	*h = append(*h, x.(blob.Metadata))
}

func (h *contentMetadataHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]

	return item
}

func (c *contentCache) sweepDirectory(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cacheStorage == nil {
		return nil
	}

	t0 := time.Now() // allow:no-inject-time

	var h contentMetadataHeap

	var totalRetainedSize int64

	err = c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		heap.Push(&h, it)
		totalRetainedSize += it.Length

		if totalRetainedSize > c.maxSizeBytes {
			oldest := heap.Pop(&h).(blob.Metadata)
			if delerr := c.cacheStorage.DeleteBlob(ctx, oldest.BlobID); delerr != nil {
				log(ctx).Warningf("unable to remove %v: %v", oldest.BlobID, delerr)
			} else {
				totalRetainedSize -= oldest.Length
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error listing cache")
	}

	log(ctx).Debugf("finished sweeping directory in %v and retained %v/%v bytes (%v %%)", time.Since(t0), totalRetainedSize, c.maxSizeBytes, 100*totalRetainedSize/c.maxSizeBytes) // allow:no-inject-time
	c.lastTotalSizeBytes = totalRetainedSize

	return nil
}

func newContentCache(ctx context.Context, st blob.Storage, caching CachingOptions, maxBytes int64, subdir string) (*contentCache, error) {
	var cacheStorage blob.Storage

	var err error

	if maxBytes > 0 && caching.CacheDirectory != "" {
		contentCacheDir := filepath.Join(caching.CacheDirectory, subdir)

		if _, err = os.Stat(contentCacheDir); os.IsNotExist(err) {
			if mkdirerr := os.MkdirAll(contentCacheDir, 0700); mkdirerr != nil {
				return nil, mkdirerr
			}
		}

		cacheStorage, err = filesystem.New(ctxutil.Detach(ctx), &filesystem.Options{
			Path:            contentCacheDir,
			DirectoryShards: []int{2},
		})
		if err != nil {
			return nil, err
		}
	}

	return newContentCacheWithCacheStorage(ctx, st, cacheStorage, maxBytes, caching, defaultTouchThreshold, defaultSweepFrequency)
}

func newContentCacheWithCacheStorage(ctx context.Context, st, cacheStorage blob.Storage, maxSizeBytes int64, caching CachingOptions, touchThreshold, sweepFrequency time.Duration) (*contentCache, error) {
	c := &contentCache{
		st:             st,
		cacheStorage:   cacheStorage,
		maxSizeBytes:   maxSizeBytes,
		hmacSecret:     append([]byte(nil), caching.HMACSecret...),
		closed:         make(chan struct{}),
		touchThreshold: touchThreshold,
		sweepFrequency: sweepFrequency,
	}

	if err := c.sweepDirectory(ctx); err != nil {
		return nil, err
	}

	c.asyncWG.Add(1)

	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
