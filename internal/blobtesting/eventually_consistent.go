package blobtesting

import (
	"context"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var eventuallyConsistentLog = logging.Module("eventually-consistent")

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
		accessTime: clock.Now(),
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
	return clock.Now().Sub(e.accessTime) < ecCacheDuration
}

type eventuallyConsistentStorage struct {
	mu sync.Mutex

	recentlyDeleted sync.Map
	listSettleTime  time.Duration

	// +checklocks:mu
	caches      []*ecFrontendCache
	realStorage blob.Storage
	timeNow     func() time.Time
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

func (s *eventuallyConsistentStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return s.realStorage.GetCapacity(ctx)
}

func (s *eventuallyConsistentStorage) IsReadOnly() bool {
	return false
}

func (s *eventuallyConsistentStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// don't bother caching partial reads
	if length >= 0 {
		return s.realStorage.GetBlob(ctx, id, offset, length, output)
	}

	c := s.randomFrontendCache()

	// see if the frontend has the blob cached
	e := c.get(id)
	if e != nil {
		if e.data == nil {
			return blob.ErrBlobNotFound
		}

		if _, err := output.Write(e.data); err != nil {
			return errors.Wrap(err, "error appending to output")
		}

		return nil
	}

	var buf gather.WriteBuffer

	// fetch from the underlying storage.
	err := s.realStorage.GetBlob(ctx, id, offset, length, &buf)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			c.put(id, nil)
		}

		return err
	}

	c.put(id, buf.ToByteSlice())

	return iocopy.JustCopy(output, buf.Bytes().Reader())
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

func (s *eventuallyConsistentStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if err := s.realStorage.PutBlob(ctx, id, data, opts); err != nil {
		return err
	}

	d, err := io.ReadAll(data.Reader())
	if err != nil {
		return errors.Wrap(err, "invalid data")
	}

	// add to frontend cache
	s.randomFrontendCache().put(id, d)

	return nil
}

func (s *eventuallyConsistentStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.randomFrontendCache().put(id, nil)

	// capture metadata before deleting
	md, err := s.realStorage.GetMetadata(ctx, id)

	if errors.Is(err, blob.ErrBlobNotFound) {
		return blob.ErrBlobNotFound
	}

	if err != nil {
		return err
	}

	if err := s.realStorage.DeleteBlob(ctx, id); err != nil {
		return err
	}

	md.Timestamp = s.timeNow()
	s.recentlyDeleted.Store(id, md)

	return nil
}

func (s *eventuallyConsistentStorage) shouldApplyInconsistency(ctx context.Context, age time.Duration, desc string) bool {
	if age < 0 {
		age = -age
	}

	if age >= s.listSettleTime {
		return false
	}

	x := age.Seconds() / s.listSettleTime.Seconds() // [0..1)

	// y=1-(x^0.3) is:
	// about 50% probability of inconsistency after 10% of listSettleTime
	// about 25% probability of inconsistency after 40% of listSettleTime
	// about 10% probability of inconsistency after 67% of listSettleTime
	// about 1% probability of inconsistency after 95% of listSettleTime

	const power = 0.3

	prob := 1 - math.Pow(x, power)

	if rand.Float64() < prob {
		eventuallyConsistentLog(ctx).Debugf("applying inconsistency %v (probability %v)", desc, prob)
		return true
	}

	return false
}

func (s *eventuallyConsistentStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	now := s.timeNow()

	if err := s.realStorage.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		if age := now.Sub(bm.Timestamp); s.shouldApplyInconsistency(ctx, age, "hide recently created "+string(bm.BlobID)) {
			return nil
		}

		return callback(bm)
	}); err != nil {
		return err
	}

	var resultErr error

	// process recently deleted items and resurrect them with some probability
	s.recentlyDeleted.Range(func(key, value interface{}) bool {
		blobID := key.(blob.ID)
		if !strings.HasPrefix(string(blobID), string(prefix)) {
			return true
		}

		bm := value.(blob.Metadata)
		if age := now.Sub(bm.Timestamp); s.shouldApplyInconsistency(ctx, age, "resurrect recently deleted "+string(bm.BlobID)) {
			if resultErr = callback(bm); resultErr != nil {
				return false
			}
		}

		return true
	})

	return resultErr
}

func (s *eventuallyConsistentStorage) Close(ctx context.Context) error {
	return s.realStorage.Close(ctx)
}

func (s *eventuallyConsistentStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.realStorage.ConnectionInfo()
}

func (s *eventuallyConsistentStorage) DisplayName() string {
	return s.realStorage.DisplayName()
}

func (s *eventuallyConsistentStorage) FlushCaches(ctx context.Context) error {
	return s.realStorage.FlushCaches(ctx)
}

func (s *eventuallyConsistentStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	return s.realStorage.ExtendBlobRetention(ctx, b, opts)
}

// NewEventuallyConsistentStorage returns an eventually-consistent storage wrapper on top
// of provided storage.
func NewEventuallyConsistentStorage(st blob.Storage, listSettleTime time.Duration, timeNow func() time.Time) blob.Storage {
	return &eventuallyConsistentStorage{
		realStorage:    st,
		caches:         make([]*ecFrontendCache, 4),
		listSettleTime: listSettleTime,
		timeNow:        timeNow,
	}
}
