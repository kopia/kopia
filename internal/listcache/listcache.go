// Package listcache defines a blob.Storage wrapper that caches results of list calls
// for short duration of time.
package listcache

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("listcache")

type listCacheStorage struct {
	blob.Storage
	cacheStorage  blob.Storage
	cacheDuration time.Duration
	cacheTimeFunc func() time.Time
	hmacSecret    []byte
	prefixes      []blob.ID
}

type cachedList struct {
	ExpireAfter time.Time       `json:"expireAfter"`
	Blobs       []blob.Metadata `json:"blobs"`
}

func (s *listCacheStorage) saveListToCache(ctx context.Context, prefix blob.ID, cl *cachedList) {
	data, err := json.Marshal(cl)
	if err != nil {
		log(ctx).Debugf("unable to marshal list cache entry: %v", err)
		return
	}

	var b gather.WriteBuffer
	defer b.Close()

	hmac.Append(gather.FromSlice(data), s.hmacSecret, &b)

	if err := s.cacheStorage.PutBlob(ctx, prefix, b.Bytes(), blob.PutOptions{}); err != nil {
		log(ctx).Debugf("unable to persist list cache entry: %v", err)
	}
}

func (s *listCacheStorage) readBlobsFromCache(ctx context.Context, prefix blob.ID) *cachedList {
	cl := &cachedList{}

	var data gather.WriteBuffer
	defer data.Close()

	if err := s.cacheStorage.GetBlob(ctx, prefix, 0, -1, &data); err != nil {
		return nil
	}

	var verified gather.WriteBuffer
	defer verified.Close()

	if err := hmac.VerifyAndStrip(data.Bytes(), s.hmacSecret, &verified); err != nil {
		log(ctx).Warnf("invalid list cache HMAC for %v, ignoring", prefix)
		return nil
	}

	if err := json.NewDecoder(verified.Bytes().Reader()).Decode(&cl); err != nil {
		log(ctx).Warnf("can't unmarshal cached list results for %v, ignoring", prefix)
		return nil
	}

	if s.cacheTimeFunc().Before(cl.ExpireAfter) {
		return cl
	}

	// list cache expired
	return nil
}

// ListBlobs implements blob.Storage and caches previous list results for a given prefix.
func (s *listCacheStorage) ListBlobs(ctx context.Context, prefix blob.ID, cb func(blob.Metadata) error) error {
	if !s.isCachedPrefix(prefix) {
		//nolint:wrapcheck
		return s.Storage.ListBlobs(ctx, prefix, cb)
	}

	cached := s.readBlobsFromCache(ctx, prefix)
	if cached == nil {
		all, err := blob.ListAllBlobs(ctx, s.Storage, prefix)
		if err != nil {
			//nolint:wrapcheck
			return err
		}

		cached = &cachedList{
			ExpireAfter: s.cacheTimeFunc().Add(s.cacheDuration),
			Blobs:       all,
		}

		s.saveListToCache(ctx, prefix, cached)
	}

	for _, v := range cached.Blobs {
		if err := cb(v); err != nil {
			return err
		}
	}

	return nil
}

// PutBlob implements blob.Storage and writes markers into local cache for all successful writes.
func (s *listCacheStorage) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	err := s.Storage.PutBlob(ctx, blobID, data, opts)
	s.invalidateAfterUpdate(ctx, blobID)

	//nolint:wrapcheck
	return err
}

func (s *listCacheStorage) FlushCaches(ctx context.Context) error {
	if err := s.Storage.FlushCaches(ctx); err != nil {
		return errors.Wrap(err, "error flushing caches")
	}

	return errors.Wrap(blob.DeleteMultiple(ctx, s.cacheStorage, s.prefixes, len(s.prefixes)), "error deleting cached lists")
}

// DeleteBlob implements blob.Storage and writes markers into local cache for all successful deletes.
func (s *listCacheStorage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	err := s.Storage.DeleteBlob(ctx, blobID)
	s.invalidateAfterUpdate(ctx, blobID)

	//nolint:wrapcheck
	return err
}

func (s *listCacheStorage) isCachedPrefix(prefix blob.ID) bool {
	for _, p := range s.prefixes {
		if prefix == p {
			return true
		}
	}

	return false
}

func (s *listCacheStorage) invalidateAfterUpdate(ctx context.Context, blobID blob.ID) {
	for _, p := range s.prefixes {
		if strings.HasPrefix(string(blobID), string(p)) {
			if err := s.cacheStorage.DeleteBlob(ctx, p); err != nil {
				log(ctx).Debugf("unable to delete cached list: %v", err)
			}
		}
	}
}

// NewWrapper returns new wrapper that ensures list consistency with local writes for the given set of blob prefixes.
// It leverages the provided local cache storage to maintain markers keeping track of recently created and deleted blobs.
func NewWrapper(st, cacheStorage blob.Storage, prefixes []blob.ID, hmacSecret []byte, duration time.Duration) blob.Storage {
	if cacheStorage == nil {
		return st
	}

	return &listCacheStorage{
		Storage:       st,
		cacheStorage:  cacheStorage,
		prefixes:      prefixes,
		cacheTimeFunc: clock.Now,
		hmacSecret:    hmacSecret,
		cacheDuration: duration,
	}
}

var _ blob.Storage = (*listCacheStorage)(nil)
