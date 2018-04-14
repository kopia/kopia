package block

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/storage"
)

const (
	sweepCacheFrequency = 1 * time.Minute
	fullListCacheItem   = "list-full"
	activeListCacheItem = "list-active"
)

type localStorageCache struct {
	st                storage.Storage
	cacheStorage      storage.Storage
	maxSizeBytes      int64
	listCacheDuration time.Duration
	hmacSecret        []byte

	mu                 sync.Mutex
	lastTotalSizeBytes int64

	closed chan struct{}
}

func (c *localStorageCache) getBlock(ctx context.Context, cacheKey string, physicalBlockID PhysicalBlockID, offset, length int64) ([]byte, error) {
	b, err := c.cacheStorage.GetBlock(ctx, cacheKey, 0, -1)
	if err == nil {
		b, err = c.verifyHMAC(b)
		if err == nil {
			// retrieved from cache and HMAC valid
			return b, nil
		}

		// ignore malformed blocks
		log.Warn().Msgf("malformed block %v: %v", cacheKey, err)
	} else if err != storage.ErrBlockNotFound {
		log.Warn().Msgf("unable to read cache %v: %v", cacheKey, err)
	}

	b, err = c.st.GetBlock(ctx, string(physicalBlockID), offset, length)
	if err == storage.ErrBlockNotFound {
		// not found in underlying storage
		return nil, err
	}

	if err == nil {
		c.writeToCacheBestEffort(ctx, cacheKey, b)
	}

	return b, err
}

func (c *localStorageCache) writeToCacheBestEffort(ctx context.Context, cacheKey string, data []byte) {
	rdr := io.MultiReader(
		bytes.NewReader(data),
		bytes.NewReader(c.computeHMAC(data)),
	)
	if err := c.cacheStorage.PutBlock(ctx, cacheKey, rdr); err != nil {
		log.Warn().Msgf("unable to write cache item %v: %v", cacheKey, err)
	}
}

func (c *localStorageCache) putBlock(ctx context.Context, blockID PhysicalBlockID, data []byte) error {
	c.deleteListCache(ctx)
	return c.st.PutBlock(ctx, string(blockID), bytes.NewReader(data))
}

func (c *localStorageCache) listIndexBlocks(ctx context.Context, full bool, extraTime time.Duration) ([]IndexInfo, error) {
	var cachedListBlockID string

	if full {
		cachedListBlockID = fullListCacheItem
	} else {
		cachedListBlockID = activeListCacheItem
	}

	ci, err := c.readBlocksFromCacheBlock(ctx, cachedListBlockID)
	if err == nil {
		expirationTime := ci.Timestamp.Add(c.listCacheDuration)
		if time.Now().Before(expirationTime) {
			log.Debug().Bool("full", full).Str("blockID", cachedListBlockID).Msg("retrieved index blocks from cache")
			return ci.Blocks, nil
		}
	} else if err != storage.ErrBlockNotFound {
		log.Warn().Str("blockID", cachedListBlockID).Err(err).Msgf("unable to open cache file")
	}

	log.Debug().Bool("full", full).Msg("listing index blocks from source")
	blocks, err := listIndexBlocksFromStorage(ctx, c.st, full, extraTime)
	if err == nil {
		c.saveListToCache(ctx, cachedListBlockID, &cachedList{
			Blocks:    blocks,
			Timestamp: time.Now(),
		})
	}

	return blocks, err
}

func (c *localStorageCache) saveListToCache(ctx context.Context, cachedListBlockID string, ci *cachedList) {
	log.Debug().Str("blockID", cachedListBlockID).Int("count", len(ci.Blocks)).Msg("saving index blocks to cache")
	if data, err := json.Marshal(ci); err == nil {
		c.writeToCacheBestEffort(ctx, cachedListBlockID, data)
	}
}

func (c *localStorageCache) readBlocksFromCacheBlock(ctx context.Context, blockID string) (*cachedList, error) {
	ci := &cachedList{}
	data, err := c.cacheStorage.GetBlock(ctx, blockID, 0, -1)
	if err != nil {
		return nil, err
	}

	data, err = c.verifyHMAC(data)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &ci); err != nil {
		return nil, fmt.Errorf("can't unmarshal cached list results: %v", err)
	}

	return ci, nil

}

func (c *localStorageCache) computeHMAC(data []byte) []byte {
	h := hmac.New(sha256.New, c.hmacSecret)
	h.Write(data) // nolint:errcheck
	return h.Sum(nil)
}

func (c *localStorageCache) verifyHMAC(b []byte) ([]byte, error) {
	if len(b) < sha256.Size {
		return nil, errors.New("invalid data - too short")
	}

	p := len(b) - sha256.Size
	data := b[0:p]
	signature := b[p:]

	validSignature := c.computeHMAC(data)
	if len(signature) != len(validSignature) {
		return nil, errors.New("invalid signature length")
	}
	if hmac.Equal(validSignature, signature) {
		return data, nil
	}

	return nil, errors.New("invalid data - corrupted")
}

func (c *localStorageCache) close() error {
	close(c.closed)
	return nil
}

func (c *localStorageCache) sweepDirectoryPeriodically(ctx context.Context) {
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

func (c *localStorageCache) sweepDirectory(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.maxSizeBytes == 0 {
		return nil
	}

	t0 := time.Now()
	log.Debug().Msg("sweeping cache")

	ctx, cancel := context.WithCancel(ctx)
	ch := c.cacheStorage.ListBlocks(ctx, "")
	defer cancel()

	var items []storage.BlockMetadata

	for it := range ch {
		if it.Error != nil {
			return fmt.Errorf("error listing cache: %v", it.Error)
		}
		items = append(items, it)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].TimeStamp.After(items[j].TimeStamp)
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

func (c *localStorageCache) deleteListCache(ctx context.Context) {
	for _, it := range []string{fullListCacheItem, activeListCacheItem} {
		if err := c.cacheStorage.DeleteBlock(ctx, it); err != nil && err != storage.ErrBlockNotFound {
			log.Warn().Err(err).Msg("unable to delete cache item")
		}
	}
}
