package content

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/blob"
)

type listCache struct {
	st                blob.Storage
	cacheFilePrefix   string
	listCacheDuration time.Duration
	hmacSecret        []byte
}

func (c *listCache) listBlobs(ctx context.Context, prefix blob.ID) ([]blob.Metadata, error) {
	if c.cacheFilePrefix != "" {
		ci, err := c.readBlobsFromCache(prefix)
		if err == nil {
			expirationTime := ci.Timestamp.Add(c.listCacheDuration)
			if clock.Now().Before(expirationTime) {
				formatLog(ctx).Debugf("list-from-cache '%v' found %v", prefix, len(ci.Blobs))
				return ci.Blobs, nil
			}
		} else if !errors.Is(err, blob.ErrBlobNotFound) {
			log(ctx).Errorf("unable to open cache file: %v", err)
		}
	}

	blobs, err := blob.ListAllBlobs(ctx, c.st, prefix)
	if err == nil {
		c.saveListToCache(ctx, prefix, &cachedList{
			Blobs:     blobs,
			Timestamp: clock.Now(),
		})
	}

	log(ctx).Debugf("listed %v index blobs with prefix %v from source", len(blobs), prefix)

	return blobs, errors.Wrap(err, "error listing blobs")
}

func (c *listCache) saveListToCache(ctx context.Context, prefix blob.ID, ci *cachedList) {
	if c.cacheFilePrefix == "" {
		return
	}

	log(ctx).Debugf("saving %v blobs with prefix %v to cache", len(ci.Blobs), prefix)

	if data, err := json.Marshal(ci); err == nil {
		b := hmac.Append(data, c.hmacSecret)
		if err := atomicfile.Write(c.cacheFilePrefix+string(prefix), bytes.NewReader(b)); err != nil {
			log(ctx).Errorf("unable to write list cache: %v", err)
		}
	}
}

func (c *listCache) deleteListCache(prefix blob.ID) {
	if c.cacheFilePrefix != "" {
		os.Remove(c.cacheFilePrefix + string(prefix)) //nolint:errcheck
	}
}

func (c *listCache) readBlobsFromCache(prefix blob.ID) (*cachedList, error) {
	ci := &cachedList{}

	fname := c.cacheFilePrefix + string(prefix)

	data, err := ioutil.ReadFile(fname) //nolint:gosec
	if err != nil {
		if os.IsNotExist(err) {
			return nil, blob.ErrBlobNotFound
		}

		return nil, errors.Wrap(err, "error reading blobs from cache")
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
		blobListCacheDir := filepath.Join(caching.CacheDirectory, "blob-list")

		if _, err := os.Stat(blobListCacheDir); os.IsNotExist(err) {
			if err := os.MkdirAll(blobListCacheDir, 0o700); err != nil {
				return nil, errors.Wrap(err, "error creating list cache directory")
			}
		}

		listCacheFilePrefix = filepath.Join(blobListCacheDir, "list-")
	}

	c := &listCache{
		st:                st,
		cacheFilePrefix:   listCacheFilePrefix,
		hmacSecret:        caching.HMACSecret,
		listCacheDuration: time.Duration(caching.MaxListCacheDurationSec) * time.Second,
	}

	return c, nil
}
