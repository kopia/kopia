// Package ownwrites defines a blob.Storage wrapper that ensures that recently-written
// blobs show up in ListBlob() results, if the underlying provider is eventually
// consistent when it comes to list results.
package ownwrites

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("ownwrites")

const (
	sweepFrequency = 5 * time.Minute

	prefixAdd    = "add"
	prefixDelete = "del"
)

//nolint:gochecknoglobals
var markerData = gather.FromSlice([]byte("marker"))

// CacheStorage implements a wrapper around blob.Storage that ensures recent local mutations
// are reflected in ListBlobs() results.
type CacheStorage struct {
	blob.Storage

	cacheStorage  blob.Storage
	cacheTimeFunc func() time.Time
	prefixes      []blob.ID
	cacheDuration time.Duration

	mu sync.Mutex

	// +checklocks:mu
	nextSweepTime time.Time
}

// ListBlobs implements blob.Storage and merges provider-returned results with cached ones.
func (s *CacheStorage) ListBlobs(ctx context.Context, prefix blob.ID, cb func(blob.Metadata) error) error {
	s.maybeSweepCache(ctx)

	cachedAdds, err := blob.ListAllBlobs(ctx, s.cacheStorage, prefixAdd+prefix)
	if err != nil {
		return errors.Wrap(err, "error listing cached blobs")
	}

	// build a map of cached adds - blobs that should appear in the repository because they were recently written.
	cachedCreatedSet := map[blob.ID]time.Time{}

	for _, bm := range cachedAdds {
		baseID := blob.ID(strings.TrimPrefix(string(bm.BlobID), prefixAdd))
		cachedCreatedSet[baseID] = bm.Timestamp
	}

	// build a map of blobs that were recently deleted locally and should not appear in list results even if the
	// provider still returns them.
	cachedDeletes, err := blob.ListAllBlobs(ctx, s.cacheStorage, prefixDelete+prefix)
	if err != nil {
		return errors.Wrap(err, "error listing cached blobs")
	}

	cachedDeletionsSet := map[blob.ID]time.Time{}

	for _, bm := range cachedDeletes {
		baseID := blob.ID(strings.TrimPrefix(string(bm.BlobID), prefixDelete))
		cachedDeletionsSet[baseID] = bm.Timestamp

		if ct, ok := cachedCreatedSet[baseID]; ok {
			if bm.Timestamp.After(ct) {
				delete(cachedCreatedSet, baseID)
			}
		}
	}

	// iterate underlying provider while removing found items from 'cachedCreatedSet'.
	if err := s.Storage.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		if _, ok := cachedDeletionsSet[bm.BlobID]; ok {
			// blob was deleted locally but still exists on the server, don't invoke callback for it.

			return nil
		}

		// delete from 'cachedCreatedSet' since the provider and cache both agree on the fact that the blob exists.
		delete(cachedCreatedSet, bm.BlobID)

		return cb(bm)
	}); err != nil {
		//nolint:wrapcheck
		return err
	}

	// Iterate remaining items in 'cachedSet' and fetch their metadata
	// from the underlying storage and invoke callback if they are found.
	//
	// This will be slower than ListBlobs() but we're expecting this set to be empty
	// most of the time because eventual consistency effects don't show up too often.
	for blobID := range cachedCreatedSet {
		bm, err := s.Storage.GetMetadata(ctx, blobID)
		if errors.Is(err, blob.ErrBlobNotFound) {
			// blob did not exist in storage, but we had the marker in cache, ignore.
			continue
		}

		if err != nil {
			//nolint:wrapcheck
			return err
		}

		if err := cb(bm); err != nil {
			return err
		}
	}

	return nil
}

// PutBlob implements blob.Storage and writes markers into local cache for all successful writes.
func (s *CacheStorage) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	err := s.Storage.PutBlob(ctx, blobID, data, opts)
	if err == nil && s.isCachedPrefix(blobID) {
		opts.GetModTime = nil

		//nolint:errcheck
		s.cacheStorage.PutBlob(ctx, prefixAdd+blobID, markerData, opts)
	}

	//nolint:wrapcheck
	return err
}

// DeleteBlob implements blob.Storage and writes markers into local cache for all successful deletes.
func (s *CacheStorage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	err := s.Storage.DeleteBlob(ctx, blobID)
	if err == nil && s.isCachedPrefix(blobID) {
		//nolint:errcheck
		s.cacheStorage.PutBlob(ctx, prefixDelete+blobID, markerData, blob.PutOptions{})
	}

	//nolint:wrapcheck
	return err
}

func (s *CacheStorage) isCachedPrefix(blobID blob.ID) bool {
	for _, p := range s.prefixes {
		if strings.HasPrefix(string(blobID), string(p)) {
			return true
		}
	}

	return false
}

func (s *CacheStorage) maybeSweepCache(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.cacheTimeFunc()
	if !now.After(s.nextSweepTime) {
		return
	}

	s.nextSweepTime = now.Add(sweepFrequency)

	if err := s.cacheStorage.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		age := now.Sub(bm.Timestamp)
		if age < s.cacheDuration {
			return nil
		}

		if err := s.cacheStorage.DeleteBlob(ctx, bm.BlobID); err != nil {
			log(ctx).Debugf("error deleting cached write marker: %v", err)
		}

		return nil
	}); err != nil {
		log(ctx).Debugf("unable to sweep cache: %v", err)
	}
}

// NewWrapper returns new wrapper that ensures list consistency with local writes for the given set of blob prefixes.
// It leverages the provided local cache storage to maintain markers keeping track of recently created and deleted blobs.
func NewWrapper(st, cacheStorage blob.Storage, prefixes []blob.ID, cacheDuration time.Duration) blob.Storage {
	if cacheStorage == nil {
		return st
	}

	return &CacheStorage{
		Storage:       st,
		cacheStorage:  cacheStorage,
		prefixes:      prefixes,
		cacheTimeFunc: clock.Now,
		cacheDuration: cacheDuration,
	}
}

var _ blob.Storage = (*CacheStorage)(nil)
