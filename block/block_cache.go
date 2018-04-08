package block

import (
	"context"
	"os"
	"time"

	"github.com/kopia/kopia/storage/filesystem"

	"github.com/kopia/kopia/storage"
)

type blockCache interface {
	getBlock(ctx context.Context, cacheKey string, physicalBlockID PhysicalBlockID, offset, length int64) ([]byte, error)
	putBlock(ctx context.Context, blockID PhysicalBlockID, data []byte) error
	listIndexBlocks(ctx context.Context, full bool) ([]IndexInfo, error)
	close() error
}

// CachingOptions specifies configuration of local cache.
type CachingOptions struct {
	CacheDirectory          string `json:"cacheDirectory,omitempty"`
	MaxCacheSizeBytes       int64  `json:"maxCacheSize,omitempty"`
	MaxListCacheDurationSec int    `json:"maxListCacheDuration,omitempty"`
	IgnoreListCache         bool   `json:"-"`
	HMACSecret              []byte `json:"-"`
}

func newBlockCache(ctx context.Context, st storage.Storage, caching CachingOptions) (blockCache, error) {
	if caching.MaxCacheSizeBytes == 0 || caching.CacheDirectory == "" {
		return nullBlockCache{st}, nil
	}

	if _, err := os.Stat(caching.CacheDirectory); os.IsNotExist(err) {
		if err := os.MkdirAll(caching.CacheDirectory, 0700); err != nil {
			return nil, err
		}
	}
	cacheStorage, err := filesystem.New(context.Background(), &filesystem.Options{
		Path: caching.CacheDirectory,
	})
	if err != nil {
		return nil, err
	}

	c := &localStorageCache{
		st:                st,
		cacheStorage:      cacheStorage,
		maxSizeBytes:      caching.MaxCacheSizeBytes,
		hmacSecret:        append([]byte(nil), caching.HMACSecret...),
		listCacheDuration: time.Duration(caching.MaxListCacheDurationSec) * time.Second,
		closed:            make(chan struct{}),
	}

	if caching.IgnoreListCache {
		c.deleteListCache(ctx)
	}

	if err := c.sweepDirectory(ctx); err != nil {
		return nil, err
	}
	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
