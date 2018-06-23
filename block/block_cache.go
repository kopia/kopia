package block

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/filesystem"
	"github.com/rs/zerolog/log"
)

const (
	sweepCacheFrequency = 1 * time.Minute
)

type blockCache struct {
	st           storage.Storage
	cacheStorage storage.Storage
	maxSizeBytes int64
	hmacSecret   []byte

	mu                 sync.Mutex
	lastTotalSizeBytes int64

	closed chan struct{}
}

func adjustCacheKey(cacheKey string) string {
	// block IDs with odd length have a single-byte prefix.
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if len(cacheKey)%2 == 1 {
		return cacheKey[1:] + cacheKey[0:1]
	}

	return cacheKey
}

func (c *blockCache) getContentBlock(ctx context.Context, cacheKey string, physicalBlockID string, offset, length int64) ([]byte, error) {
	cacheKey = adjustCacheKey(cacheKey)

	useCache := shouldUseBlockCache(ctx) && c.cacheStorage != nil
	if useCache {
		b, err := c.cacheStorage.GetBlock(ctx, cacheKey, 0, -1)
		if err == nil {
			b, err = verifyAndStripHMAC(b, c.hmacSecret)
			if err == nil {
				// retrieved from cache and HMAC valid
				return b, nil
			}

			// ignore malformed blocks
			log.Warn().Msgf("malformed block %v: %v", cacheKey, err)
		} else if err != storage.ErrBlockNotFound {
			log.Warn().Msgf("unable to read cache %v: %v", cacheKey, err)
		}
	}

	b, err := c.st.GetBlock(ctx, physicalBlockID, offset, length)
	if err == storage.ErrBlockNotFound {
		// not found in underlying storage
		return nil, err
	}

	if err == nil && useCache {
		if puterr := c.cacheStorage.PutBlock(ctx, cacheKey, appendHMAC(b, c.hmacSecret)); puterr != nil {
			log.Warn().Msgf("unable to write cache item %v: %v", cacheKey, puterr)
		}
	}

	return b, err
}

func (c *blockCache) close() {
	close(c.closed)
}

func (c *blockCache) sweepDirectoryPeriodically(ctx context.Context) {
	for {
		select {
		case <-c.closed:
			return

		case <-time.After(sweepCacheFrequency):
			err := c.sweepDirectory(ctx)
			if err != nil {
				log.Printf("warning: blockCache sweep failed: %v", err)
			}
		}
	}
}

func (c *blockCache) sweepDirectory(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cacheStorage == nil {
		return nil
	}

	t0 := time.Now()
	log.Debug().Msg("sweeping cache")

	var items []storage.BlockMetadata
	err = c.cacheStorage.ListBlocks(ctx, "", func(it storage.BlockMetadata) error {
		items = append(items, it)
		return nil
	})

	if err != nil {
		return fmt.Errorf("error listing cache: %v", err)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Timestamp.After(items[j].Timestamp)
	})

	var totalRetainedSize int64
	for _, it := range items {
		if totalRetainedSize > c.maxSizeBytes {
			if err := c.cacheStorage.DeleteBlock(ctx, it.BlockID); err != nil {
				log.Warn().Msgf("unable to remove %v: %v", it.BlockID, err)
			}
		} else {
			totalRetainedSize += it.Length
		}
	}
	log.Debug().Msgf("finished sweeping directory in %v and retained %v/%v bytes (%v %%)", time.Since(t0), totalRetainedSize, c.maxSizeBytes, 100*totalRetainedSize/c.maxSizeBytes)
	c.lastTotalSizeBytes = totalRetainedSize
	return nil
}

func newBlockCache(ctx context.Context, st storage.Storage, caching CachingOptions) (*blockCache, error) {
	var cacheStorage storage.Storage
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

	c := &blockCache{
		st:           st,
		cacheStorage: cacheStorage,
		maxSizeBytes: caching.MaxCacheSizeBytes,
		hmacSecret:   append([]byte(nil), caching.HMACSecret...),
		closed:       make(chan struct{}),
	}

	if err := c.sweepDirectory(ctx); err != nil {
		return nil, err
	}
	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
