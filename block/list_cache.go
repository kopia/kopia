package block

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/kopia/repo/storage"
)

type listCache struct {
	st                storage.Storage
	cacheFile         string
	listCacheDuration time.Duration
	hmacSecret        []byte
}

func (c *listCache) listIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	if c.cacheFile != "" {
		ci, err := c.readBlocksFromCache(ctx)
		if err == nil {
			expirationTime := ci.Timestamp.Add(c.listCacheDuration)
			if time.Now().Before(expirationTime) {
				log.Debugf("retrieved list of index blocks from cache")
				return ci.Blocks, nil
			}
		} else if err != storage.ErrBlockNotFound {
			log.Warningf("unable to open cache file: %v", err)
		}
	}

	blocks, err := listIndexBlocksFromStorage(ctx, c.st)
	if err == nil {
		c.saveListToCache(ctx, &cachedList{
			Blocks:    blocks,
			Timestamp: time.Now(),
		})
	}
	log.Debugf("found %v index blocks from source", len(blocks))

	return blocks, err
}

func (c *listCache) saveListToCache(ctx context.Context, ci *cachedList) {
	if c.cacheFile == "" {
		return
	}
	log.Debugf("saving index blocks to cache: %v", len(ci.Blocks))
	if data, err := json.Marshal(ci); err == nil {
		mySuffix := fmt.Sprintf(".tmp-%v-%v", os.Getpid(), time.Now().UnixNano())
		if err := ioutil.WriteFile(c.cacheFile+mySuffix, appendHMAC(data, c.hmacSecret), 0600); err != nil {
			log.Warningf("unable to write list cache: %v", err)
		}
		os.Rename(c.cacheFile+mySuffix, c.cacheFile) //nolint:errcheck
		os.Remove(c.cacheFile + mySuffix)            //nolint:errcheck
	}
}

func (c *listCache) deleteListCache(ctx context.Context) {
	if c.cacheFile != "" {
		os.Remove(c.cacheFile) //nolint:errcheck
	}
}

func (c *listCache) readBlocksFromCache(ctx context.Context) (*cachedList, error) {
	if !shouldUseListCache(ctx) {
		return nil, storage.ErrBlockNotFound
	}

	ci := &cachedList{}

	data, err := ioutil.ReadFile(c.cacheFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrBlockNotFound
		}

		return nil, err
	}

	data, err = verifyAndStripHMAC(data, c.hmacSecret)
	if err != nil {
		return nil, fmt.Errorf("invalid file %v: %v", c.cacheFile, err)
	}

	if err := json.Unmarshal(data, &ci); err != nil {
		return nil, fmt.Errorf("can't unmarshal cached list results: %v", err)
	}

	return ci, nil

}

func newListCache(ctx context.Context, st storage.Storage, caching CachingOptions) (*listCache, error) {
	var listCacheFile string

	if caching.CacheDirectory != "" {
		listCacheFile = filepath.Join(caching.CacheDirectory, "list")

		if _, err := os.Stat(caching.CacheDirectory); os.IsNotExist(err) {
			if err := os.MkdirAll(caching.CacheDirectory, 0700); err != nil {
				return nil, err
			}
		}
	}

	c := &listCache{
		st:                st,
		cacheFile:         listCacheFile,
		hmacSecret:        caching.HMACSecret,
		listCacheDuration: time.Duration(caching.MaxListCacheDurationSec) * time.Second,
	}

	if caching.IgnoreListCache {
		c.deleteListCache(ctx)
	}

	return c, nil
}
