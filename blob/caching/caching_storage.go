// Package caching implements a caching wrapper around another Storage.
package caching

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/filesystem"
)

var (
	dbBucketBlocks = []byte("Blocks")
	currentTime    func() int64
)

const (
	defaultCacheSizeBytes = 100000000
)

type cachingStorage struct {
	master    blob.Storage
	cache     blob.Storage
	db        *bolt.DB
	sizeBytes int64

	*lockMap
}

func defaultGetCurrentTime() int64 {
	return time.Now().UnixNano()
}

var getCurrentTime = defaultGetCurrentTime

func (c *cachingStorage) getCacheEntry(block string) (result blockCacheEntry, ok bool) {
	c.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(dbBucketBlocks)
		if b == nil {
			return nil
		}

		cacheKey := []byte(block)
		entryBytes := b.Get(cacheKey)
		var e blockCacheEntry

		if entryBytes != nil {
			err := e.deserialize(entryBytes)
			if err == nil {
				//log.Printf("got cache entry: %v %#v", block, e)
				result = e
				ok = true

				e.accessTime = getCurrentTime()

				return b.Put(cacheKey, e.serialize())
			}

			log.Printf("WARNING: Can't read cache entry: %v", err)
		}

		return nil
	})
	return
}

func (c *cachingStorage) setCacheEntrySize(block string, size int64) {
	c.db.Update(func(t *bolt.Tx) error {
		b, err := t.CreateBucketIfNotExists(dbBucketBlocks)
		if err != nil {
			return err
		}

		e := blockCacheEntry{
			accessTime: getCurrentTime(),
			size:       size,
		}

		// log.Printf("updating cache entry %v with size %x", block, size)
		return b.Put([]byte(block), e.serialize())
	})
}

func (c *cachingStorage) removeCacheEntry(block string) {
	c.db.Update(func(t *bolt.Tx) error {
		b, err := t.CreateBucketIfNotExists(dbBucketBlocks)
		if err != nil {
			return err
		}

		return b.Delete([]byte(block))
	})
}

func (c *cachingStorage) BlockSize(id string) (int64, error) {
	if entry, ok := c.getCacheEntry(id); ok {
		if entry.exists() {
			return entry.size, nil
		}

		return 0, blob.ErrBlockNotFound
	}

	c.Lock(id)
	defer c.Unlock(id)

	l, err := c.master.BlockSize(id)
	if err != nil {
		if err == blob.ErrBlockNotFound {
			c.setCacheEntrySize(id, sizeDoesNotExists)
		}
		return 0, err
	}

	c.setCacheEntrySize(id, l)
	return l, nil
}

func (c *cachingStorage) DeleteBlock(id string) error {
	c.Lock(id)
	defer c.Unlock(id)

	// Remove from cache first.
	c.cache.DeleteBlock(id)

	if err := c.master.DeleteBlock(id); err != nil {
		return err
	}
	c.setCacheEntrySize(id, sizeDoesNotExists)

	return nil
}

func (c *cachingStorage) GetBlock(id string, offset, length int64) ([]byte, error) {
	c.Lock(id)
	defer c.Unlock(id)

	if blockCacheEntry, ok := c.getCacheEntry(id); ok {
		if !blockCacheEntry.exists() {
			return nil, blob.ErrBlockNotFound
		}

		v, err := c.cache.GetBlock(id, offset, length)
		if err == nil {
			return v, nil
		}
	}

	// Download from master
	b, err := c.master.GetBlock(id, 0, -1)

	if err == nil {
		l := int64(len(b))
		c.cache.PutBlock(id, b, blob.PutOptionsOverwrite)
		c.setCacheEntrySize(id, l)
	} else if err == blob.ErrBlockNotFound {
		c.setCacheEntrySize(id, sizeDoesNotExists)
	}

	return b, err
}

func (c *cachingStorage) PutBlock(id string, data []byte, options blob.PutOptions) error {
	c.Lock(id)
	defer c.Unlock(id)

	// Remove from cache first.
	c.cache.DeleteBlock(id)
	c.removeCacheEntry(id)

	return c.master.PutBlock(id, data, options)
}

func (c *cachingStorage) ListBlocks(prefix string) (chan blob.BlockMetadata, blob.CancelFunc) {
	return c.master.ListBlocks(prefix)
}

func (c *cachingStorage) Close() error {
	if c.db != nil {
		c.db.Close()
		c.db = nil
	}

	if c.cache != nil {
		c.cache.Close()
		c.cache = nil
	}

	if c.master != nil {
		c.master.Close()
		c.master = nil
	}
	return nil
}

// Options provides options for the caching storage wrapper
type Options struct {
	CacheDir       string `json:"cacheDir"`
	CacheSizeBytes int64  `json:"cacheSizeBytes"`
}

// NewWrapper creates new caching storage wrapper.
func NewWrapper(ctx context.Context, master blob.Storage, options *Options) (blob.Storage, error) {
	if options.CacheDir == "" {
		return nil, fmt.Errorf("Cache directory must be specified")
	}
	cacheDataDir := filepath.Join(options.CacheDir, "data")
	cacheDBFile := filepath.Join(options.CacheDir, "cache.db")

	os.MkdirAll(cacheDataDir, 0700)

	db, err := bolt.Open(cacheDBFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("cannot open cache database: %v", err)
	}

	cs, err := filesystem.New(ctx, &filesystem.Options{
		Path: cacheDataDir,
	})

	if err != nil {
		return nil, fmt.Errorf("cannot open cache directory: %v", err)
	}

	sizeBytes := options.CacheSizeBytes
	if sizeBytes == 0 {
		sizeBytes = defaultCacheSizeBytes
	}

	s := &cachingStorage{
		master:    master,
		cache:     cs,
		db:        db,
		sizeBytes: sizeBytes,
		lockMap:   newLockMap(),
	}

	return s, nil
}

var _ blob.Storage = &cachingStorage{}
