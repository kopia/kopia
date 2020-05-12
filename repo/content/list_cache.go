package content

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/blob"
)

type listCache struct {
	st                blob.Storage
	cacheFilePrefix   string
	listCacheDuration time.Duration
	hmacSecret        []byte
}

func (c *listCache) listIndexBlobs(ctx context.Context, prefix blob.ID) ([]blob.Metadata, error) {
	if c.cacheFilePrefix != "" {
		ci, err := c.readBlobsFromCache(ctx, prefix)
		if err == nil {
			expirationTime := ci.Timestamp.Add(c.listCacheDuration)
			if time.Now().Before(expirationTime) { // allow:no-inject-time
				log(ctx).Debugf("retrieved list of %v '%v' index blobs from cache", len(ci.Blobs), prefix)
				return ci.Blobs, nil
			}
		} else if err != blob.ErrBlobNotFound {
			log(ctx).Warningf("unable to open cache file: %v", err)
		}
	}

	blobs, err := blob.ListAllBlobs(ctx, c.st, prefix)
	if err == nil {
		c.saveListToCache(ctx, prefix, &cachedList{
			Blobs:     blobs,
			Timestamp: time.Now(), // allow:no-inject-time
		})
	}

	log(ctx).Debugf("found %v index blobs from source", len(blobs))

	return blobs, err
}

func (c *listCache) saveListToCache(ctx context.Context, prefix blob.ID, ci *cachedList) {
	if c.cacheFilePrefix == "" {
		return
	}

	log(ctx).Debugf("saving %v blobs with prefix %v to cache", len(ci.Blobs), prefix)

	if data, err := json.Marshal(ci); err == nil {
		b := hmac.Append(data, c.hmacSecret)
		if err := atomic.WriteFile(c.cacheFilePrefix+string(prefix), bytes.NewReader(b)); err != nil {
			log(ctx).Warningf("unable to write list cache: %v", err)
		}
	}
}

func (c *listCache) deleteListCache(prefix blob.ID) {
	if c.cacheFilePrefix != "" {
		os.Remove(c.cacheFilePrefix + string(prefix)) //nolint:errcheck
	}
}

func (c *listCache) readBlobsFromCache(ctx context.Context, prefix blob.ID) (*cachedList, error) {
	if !shouldUseListCache(ctx) {
		return nil, blob.ErrBlobNotFound
	}

	ci := &cachedList{}

	fname := c.cacheFilePrefix + string(prefix)

	data, err := ioutil.ReadFile(fname) //nolint:gosec
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrBlobNotFound
		}

		return nil, err
	}

	data, err = hmac.VerifyAndStrip(data, c.hmacSecret)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid file %v", fname)
	}

	if err := json.Unmarshal(data, &ci); err != nil {
		return nil, errors.Wrap(err, "can't unmarshal cached list results")
	}

	return ci, nil
}

type cachedList struct {
	Timestamp time.Time       `json:"timestamp"`
	Blobs     []blob.Metadata `json:"blobs"`
}

func newListCache(st blob.Storage, caching *CachingOptions) (*listCache, error) {
	var listCacheFilePrefix string

	if caching.CacheDirectory != "" {
		listCacheFilePrefix = filepath.Join(caching.CacheDirectory, "blob-list-")

		if _, err := os.Stat(caching.CacheDirectory); os.IsNotExist(err) {
			if err := os.MkdirAll(caching.CacheDirectory, 0700); err != nil {
				return nil, err
			}
		}
	}

	c := &listCache{
		st:                st,
		cacheFilePrefix:   listCacheFilePrefix,
		hmacSecret:        caching.HMACSecret,
		listCacheDuration: time.Duration(caching.MaxListCacheDurationSec) * time.Second,
	}

	return c, nil
}
