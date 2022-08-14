package format

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/cachedir"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
)

// DefaultRepositoryBlobCacheDuration is the duration for which we treat cached kopia.repository
// as valid.
const DefaultRepositoryBlobCacheDuration = 15 * time.Minute

// BlobCache encapsulates cache for format blobs.
type BlobCache interface {
	Get(ctx context.Context, blobID blob.ID) ([]byte, time.Time, bool)
	Put(ctx context.Context, blobID blob.ID, data []byte) (time.Time, error)
	Remove(ctx context.Context, ids []blob.ID)
}

type nullCache struct{}

func (nullCache) Get(ctx context.Context, blobID blob.ID) ([]byte, time.Time, bool) {
	return nil, time.Time{}, false
}

func (nullCache) Put(ctx context.Context, blobID blob.ID, data []byte) (time.Time, error) {
	return clock.Now(), nil
}

func (nullCache) Remove(ctx context.Context, ids []blob.ID) {
}

type inMemoryCache struct {
	timeNow func() time.Time // +checklocksignore

	mu sync.Mutex
	// +checklocks:mu
	data map[blob.ID][]byte
	// +checklocks:mu
	times map[blob.ID]time.Time
}

func (c *inMemoryCache) Get(ctx context.Context, blobID blob.ID) ([]byte, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.data[blobID]
	if ok {
		return data, c.times[blobID], true
	}

	return nil, time.Time{}, false
}

func (c *inMemoryCache) Put(ctx context.Context, blobID blob.ID, data []byte) (time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[blobID] = data
	c.times[blobID] = c.timeNow()

	return c.times[blobID], nil
}

func (c *inMemoryCache) Remove(ctx context.Context, ids []blob.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, blobID := range ids {
		delete(c.data, blobID)
		delete(c.times, blobID)
	}
}

type onDiskCache struct {
	cacheDirectory string
}

func (c *onDiskCache) Get(ctx context.Context, blobID blob.ID) ([]byte, time.Time, bool) {
	cachedFile := filepath.Join(c.cacheDirectory, string(blobID))

	cst, err := os.Stat(cachedFile)
	if err != nil {
		return nil, time.Time{}, false
	}

	cacheMTime := cst.ModTime()

	//nolint:gosec
	data, err := os.ReadFile(cachedFile)

	return data, cacheMTime, err == nil
}

func (c *onDiskCache) Put(ctx context.Context, blobID blob.ID, data []byte) (time.Time, error) {
	cachedFile := filepath.Join(c.cacheDirectory, string(blobID))

	// optimistically assume cache directory exist, create it if not
	if err := atomicfile.Write(cachedFile, bytes.NewReader(data)); err != nil {
		if err := os.MkdirAll(c.cacheDirectory, cache.DirMode); err != nil && !os.IsExist(err) {
			return time.Time{}, errors.Wrap(err, "unable to create cache directory")
		}

		if err := cachedir.WriteCacheMarker(c.cacheDirectory); err != nil {
			return time.Time{}, errors.Wrap(err, "unable to write cache directory marker")
		}

		if err := atomicfile.Write(cachedFile, bytes.NewReader(data)); err != nil {
			return time.Time{}, errors.Wrapf(err, "unable to write to cache: %v", string(blobID))
		}
	}

	cst, err := os.Stat(cachedFile)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "unable to open cache file")
	}

	return cst.ModTime(), nil
}

func (c *onDiskCache) Remove(ctx context.Context, ids []blob.ID) {
	for _, blobID := range ids {
		fname := filepath.Join(c.cacheDirectory, string(blobID))
		log(ctx).Infof("deleting %v", fname)

		if err := os.Remove(fname); err != nil && !os.IsNotExist(err) {
			log(ctx).Debugf("unable to remove cached repository format blob: %v", err)
		}
	}
}

// NewDiskCache returns on-disk blob cache.
func NewDiskCache(cacheDir string) BlobCache {
	return &onDiskCache{cacheDir}
}

// NewMemoryBlobCache returns in-memory blob cache.
func NewMemoryBlobCache(timeNow func() time.Time) BlobCache {
	return &inMemoryCache{
		timeNow: timeNow,
		data:    map[blob.ID][]byte{},
		times:   map[blob.ID]time.Time{},
	}
}

// NewFormatBlobCache creates an implementationof BlobCache for particular cache settings.
func NewFormatBlobCache(cacheDir string, validDuration time.Duration, timeNow func() time.Time) BlobCache {
	if cacheDir != "" {
		return NewDiskCache(cacheDir)
	}

	if validDuration > 0 {
		return NewMemoryBlobCache(timeNow)
	}

	return &nullCache{}
}

var (
	_ BlobCache = (*nullCache)(nil)
	_ BlobCache = (*inMemoryCache)(nil)
	_ BlobCache = (*onDiskCache)(nil)
)
