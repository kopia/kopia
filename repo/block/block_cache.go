package block

import (
	"container/heap"
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

const (
	defaultSweepFrequency = 1 * time.Minute
	defaultTouchThreshold = 10 * time.Minute
)

type blockCache struct {
	st             blob.Storage
	cacheStorage   blob.Storage
	maxSizeBytes   int64
	hmacSecret     []byte
	sweepFrequency time.Duration
	touchThreshold time.Duration

	mu                 sync.Mutex
	lastTotalSizeBytes int64

	closed chan struct{}
}

type blockToucher interface {
	TouchBlob(ctx context.Context, blockID blob.ID, threshold time.Duration) error
}

func adjustCacheKey(cacheKey blob.ID) blob.ID {
	// block IDs with odd length have a single-byte prefix.
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if len(cacheKey)%2 == 1 {
		return cacheKey[1:] + cacheKey[0:1]
	}

	return cacheKey
}

func (c *blockCache) getContentBlock(ctx context.Context, cacheKey blob.ID, blobID blob.ID, offset, length int64) ([]byte, error) {
	cacheKey = adjustCacheKey(cacheKey)

	useCache := shouldUseBlockCache(ctx) && c.cacheStorage != nil
	if useCache {
		if b := c.readAndVerifyCacheBlock(ctx, cacheKey); b != nil {
			return b, nil
		}
	}

	b, err := c.st.GetBlob(ctx, blobID, offset, length)
	if err == blob.ErrBlobNotFound {
		// not found in underlying storage
		return nil, err
	}

	if err == nil && useCache {
		if puterr := c.cacheStorage.PutBlob(ctx, cacheKey, appendHMAC(b, c.hmacSecret)); puterr != nil {
			log.Warningf("unable to write cache item %v: %v", cacheKey, puterr)
		}
	}

	return b, err
}

func (c *blockCache) readAndVerifyCacheBlock(ctx context.Context, cacheKey blob.ID) []byte {
	b, err := c.cacheStorage.GetBlob(ctx, cacheKey, 0, -1)
	if err == nil {
		b, err = verifyAndStripHMAC(b, c.hmacSecret)
		if err == nil {
			if t, ok := c.cacheStorage.(blockToucher); ok {
				t.TouchBlob(ctx, cacheKey, c.touchThreshold) //nolint:errcheck
			}

			// retrieved from cache and HMAC valid
			return b
		}

		// ignore malformed blocks
		log.Warningf("malformed block %v: %v", cacheKey, err)
		return nil
	}

	if err != blob.ErrBlobNotFound {
		log.Warningf("unable to read cache %v: %v", cacheKey, err)
	}
	return nil
}

func (c *blockCache) close() {
	close(c.closed)
}

func (c *blockCache) sweepDirectoryPeriodically(ctx context.Context) {
	for {
		select {
		case <-c.closed:
			return

		case <-time.After(c.sweepFrequency):
			err := c.sweepDirectory(ctx)
			if err != nil {
				log.Warningf("blockCache sweep failed: %v", err)
			}
		}
	}
}

// A blockMetadataHeap implements heap.Interface and holds blob.Metadata.
type blockMetadataHeap []blob.Metadata

func (h blockMetadataHeap) Len() int { return len(h) }

func (h blockMetadataHeap) Less(i, j int) bool {
	return h[i].Timestamp.Before(h[j].Timestamp)
}

func (h blockMetadataHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *blockMetadataHeap) Push(x interface{}) {
	*h = append(*h, x.(blob.Metadata))
}

func (h *blockMetadataHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (c *blockCache) sweepDirectory(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cacheStorage == nil {
		return nil
	}

	t0 := time.Now()

	var h blockMetadataHeap
	var totalRetainedSize int64

	err = c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		heap.Push(&h, it)
		totalRetainedSize += it.Length

		if totalRetainedSize > c.maxSizeBytes {
			oldest := heap.Pop(&h).(blob.Metadata)
			if delerr := c.cacheStorage.DeleteBlob(ctx, oldest.BlobID); delerr != nil {
				log.Warningf("unable to remove %v: %v", oldest.BlobID, delerr)
			} else {
				totalRetainedSize -= oldest.Length
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error listing cache")
	}

	log.Debugf("finished sweeping directory in %v and retained %v/%v bytes (%v %%)", time.Since(t0), totalRetainedSize, c.maxSizeBytes, 100*totalRetainedSize/c.maxSizeBytes)
	c.lastTotalSizeBytes = totalRetainedSize
	return nil
}

func newBlockCache(ctx context.Context, st blob.Storage, caching CachingOptions) (*blockCache, error) {
	var cacheStorage blob.Storage
	var err error

	if caching.MaxCacheSizeBytes > 0 && caching.CacheDirectory != "" {
		blockCacheDir := filepath.Join(caching.CacheDirectory, "blocks")

		if _, err = os.Stat(blockCacheDir); os.IsNotExist(err) {
			if err = os.MkdirAll(blockCacheDir, 0700); err != nil {
				return nil, err
			}
		}

		cacheStorage, err = filesystem.New(context.Background(), &filesystem.Options{
			Path:            blockCacheDir,
			DirectoryShards: []int{2},
		})
		if err != nil {
			return nil, err
		}
	}

	return newBlockCacheWithCacheStorage(ctx, st, cacheStorage, caching, defaultTouchThreshold, defaultSweepFrequency)
}

func newBlockCacheWithCacheStorage(ctx context.Context, st, cacheStorage blob.Storage, caching CachingOptions, touchThreshold time.Duration, sweepFrequency time.Duration) (*blockCache, error) {
	c := &blockCache{
		st:             st,
		cacheStorage:   cacheStorage,
		maxSizeBytes:   caching.MaxCacheSizeBytes,
		hmacSecret:     append([]byte(nil), caching.HMACSecret...),
		closed:         make(chan struct{}),
		touchThreshold: touchThreshold,
		sweepFrequency: sweepFrequency,
	}

	if err := c.sweepDirectory(ctx); err != nil {
		return nil, err
	}
	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
