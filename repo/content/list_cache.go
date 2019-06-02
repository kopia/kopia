package content

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

type listCache struct {
	st                blob.Storage
	cacheFile         string
	listCacheDuration time.Duration
	hmacSecret        []byte
}

func (c *listCache) listIndexBlobs(ctx context.Context) ([]IndexBlobInfo, error) {
	if c.cacheFile != "" {
		ci, err := c.readContentsFromCache(ctx)
		if err == nil {
			expirationTime := ci.Timestamp.Add(c.listCacheDuration)
			if time.Now().Before(expirationTime) {
				log.Debugf("retrieved list of index blobs from cache")
				return ci.Contents, nil
			}
		} else if err != blob.ErrBlobNotFound {
			log.Warningf("unable to open cache file: %v", err)
		}
	}

	contents, err := listIndexBlobsFromStorage(ctx, c.st)
	if err == nil {
		c.saveListToCache(ctx, &cachedList{
			Contents:  contents,
			Timestamp: time.Now(),
		})
	}
	log.Debugf("found %v index blobs from source", len(contents))

	return contents, err
}

func (c *listCache) saveListToCache(ctx context.Context, ci *cachedList) {
	if c.cacheFile == "" {
		return
	}
	log.Debugf("saving index blobs to cache: %v", len(ci.Contents))
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

func (c *listCache) readContentsFromCache(ctx context.Context) (*cachedList, error) {
	if !shouldUseListCache(ctx) {
		return nil, blob.ErrBlobNotFound
	}

	ci := &cachedList{}

	data, err := ioutil.ReadFile(c.cacheFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrBlobNotFound
		}

		return nil, err
	}

	data, err = verifyAndStripHMAC(data, c.hmacSecret)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid file %v", c.cacheFile)
	}

	if err := json.Unmarshal(data, &ci); err != nil {
		return nil, errors.Wrap(err, "can't unmarshal cached list results")
	}

	return ci, nil

}

func newListCache(ctx context.Context, st blob.Storage, caching CachingOptions) (*listCache, error) {
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
