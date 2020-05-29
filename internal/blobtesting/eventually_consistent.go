package blobtesting

import (
	"context"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const ecCacheDuration = 5 * time.Second

// ecFrontendCache is an instance of cache, which simulates cloud storage frontend
// with its own in-memory state. This causes eventual consistency when a client uses
// different instances of the cache for read and writes.
type ecFrontendCache struct {
	mu            sync.Mutex
	cachedEntries map[blob.ID]*ecCacheEntry
}

func (c *ecFrontendCache) get(id blob.ID) *ecCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	e := c.cachedEntries[id]
	if e != nil && !e.isValid() {
		c.sweepLocked()
		return nil
	}

	return e
}

func (c *ecFrontendCache) put(id blob.ID, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sweepLocked()

	if c.cachedEntries == nil {
		c.cachedEntries = map[blob.ID]*ecCacheEntry{}
	}

	if data != nil {
		// clone data before storage
		data = append([]byte(nil), data...)
	}

	c.cachedEntries[id] = &ecCacheEntry{
		accessTime: time.Now(),
		data:       data,
	}
}

func (c *ecFrontendCache) sweepLocked() {
	for k, v := range c.cachedEntries {
		if !v.isValid() {
			delete(c.cachedEntries, k)
		}
	}
}

type ecCacheEntry struct {
	data []byte

	accessTime time.Time
}

func (e *ecCacheEntry) isValid() bool {
	return time.Since(e.accessTime) < ecCacheDuration
}

type eventuallyConsistentStorage struct {
	mu sync.Mutex

	listDropProbability float64

	caches      []*ecFrontendCache
	realStorage blob.Storage
}

func (s *eventuallyConsistentStorage) randomFrontendCache() *ecFrontendCache {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := rand.Intn(len(s.caches))

	if s.caches[n] == nil {
		s.caches[n] = &ecFrontendCache{}
	}

	return s.caches[n]
}

func (s *eventuallyConsistentStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64) ([]byte, error) {
	// don't bother caching partial reads
	if length >= 0 {
		return s.realStorage.GetBlob(ctx, id, offset, length)
	}

	c := s.randomFrontendCache()

	// see if the frontend has the blob cached
	e := c.get(id)
	if e != nil {
		if e.data == nil {
			return nil, blob.ErrBlobNotFound
		}

		return append([]byte(nil), e.data...), nil
	}

	// fetch from the underlying storage.
	v, err := s.realStorage.GetBlob(ctx, id, offset, length)
	if err != nil {
		if err == blob.ErrBlobNotFound {
			c.put(id, nil)
		}

		return nil, err
	}

	c.put(id, v)

	return v, nil
}

func (s *eventuallyConsistentStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	c := s.randomFrontendCache()

	// see if the frontend has cached blob deleted/not exists
	e := c.get(id)
	if e != nil {
		if e.data == nil {
			return blob.Metadata{}, blob.ErrBlobNotFound
		}
	}

	// fetch from the underlying storage.
	return s.realStorage.GetMetadata(ctx, id)
}

func (s *eventuallyConsistentStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes) error {
	if err := s.realStorage.PutBlob(ctx, id, data); err != nil {
		return err
	}

	d, err := ioutil.ReadAll(data.Reader())
	if err != nil {
		return errors.Wrap(err, "invalid data")
	}

	// add to frontend cache
	s.randomFrontendCache().put(id, d)

	return nil
}

func (s *eventuallyConsistentStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.randomFrontendCache().put(id, nil)

	if err := s.realStorage.DeleteBlob(ctx, id); err != nil {
		return err
	}

	return nil
}

func (s *eventuallyConsistentStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	return s.realStorage.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		e := s.randomFrontendCache().get(bm.BlobID)
		if e != nil {
			// item recently manipulated by the cache, skip from the results with some
			// probability
			if rand.Float64() < s.listDropProbability {
				// skip callback if locally deleted
				return nil
			}
		}

		return callback(bm)
	})
}

func (s *eventuallyConsistentStorage) Close(ctx context.Context) error {
	return s.realStorage.Close(ctx)
}

func (s *eventuallyConsistentStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.realStorage.ConnectionInfo()
}

// NewEventuallyConsistentStorage returns an eventually-consistent storage wrapper on top
// of provided storage.
func NewEventuallyConsistentStorage(st blob.Storage, listDropProbability float64) blob.Storage {
	return &eventuallyConsistentStorage{
		realStorage:         st,
		caches:              make([]*ecFrontendCache, 4),
		listDropProbability: listDropProbability,
	}
}
