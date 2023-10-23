package cache

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/repo/blob"
)

// ContentCache caches contents stored in pack blobs.
type ContentCache interface {
	Close(ctx context.Context)
	GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error
	PrefetchBlob(ctx context.Context, blobID blob.ID) error
	CacheStorage() Storage
}

// Options encapsulates all content cache options.
type Options struct {
	BaseCacheDirectory string
	CacheSubDir        string
	Storage            Storage // force particular storage, used for testing
	HMACSecret         []byte
	FetchFullBlobs     bool
	Sweep              SweepSettings
	TimeNow            func() time.Time
}

type contentCacheImpl struct {
	pc             *PersistentCache
	st             blob.Storage
	fetchFullBlobs bool
}

// ContentIDCacheKey computes the cache key for the provided content ID.
func ContentIDCacheKey(contentID string) string {
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if contentID[0] >= 'g' && contentID[0] <= 'z' {
		return contentID[1:] + contentID[0:1]
	}

	return contentID
}

// BlobIDCacheKey computes the cache key for the provided blob ID.
func BlobIDCacheKey(id blob.ID) string {
	return string(id[1:] + id[0:1])
}

func (c *contentCacheImpl) GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	if c.fetchFullBlobs {
		return c.getContentFromFullBlob(ctx, blobID, offset, length, output)
	}

	return c.getContentFromFullOrPartialBlob(ctx, contentID, blobID, offset, length, output)
}

func (c *contentCacheImpl) getContentFromFullBlob(ctx context.Context, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	c.pc.exclusiveLock(string(blobID))
	defer c.pc.exclusiveUnlock(string(blobID))

	// check again to see if we perhaps lost the race and the data is now in cache.
	if c.pc.GetPartial(ctx, BlobIDCacheKey(blobID), offset, length, output) {
		return nil
	}

	var blobData gather.WriteBuffer
	defer blobData.Close()

	if err := c.fetchBlobInternal(ctx, blobID, &blobData); err != nil {
		return err
	}

	if offset == 0 && length == -1 {
		_, err := blobData.Bytes().WriteTo(output)

		return errors.Wrap(err, "error copying results")
	}

	if offset < 0 || offset+length > int64(blobData.Length()) {
		return errors.Errorf("invalid (offset=%v,length=%v) for blob %q of size %v", offset, length, blobID, blobData.Length())
	}

	output.Reset()

	impossible.PanicOnError(blobData.AppendSectionTo(output, int(offset), int(length)))

	return nil
}

func (c *contentCacheImpl) fetchBlobInternal(ctx context.Context, blobID blob.ID, blobData *gather.WriteBuffer) error {
	// read the entire blob
	if err := c.st.GetBlob(ctx, blobID, 0, -1, blobData); err != nil {
		c.pc.reportMissError()

		return errors.Wrapf(err, "failed to get blob with ID %s", blobID)
	}

	c.pc.reportMissBytes(int64(blobData.Length()))

	// store the whole blob in the cache.
	c.pc.Put(ctx, BlobIDCacheKey(blobID), blobData.Bytes())

	return nil
}

func (c *contentCacheImpl) getContentFromFullOrPartialBlob(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	// acquire shared lock on a blob, PrefetchBlob will acquire exclusive lock here.
	c.pc.sharedLock(string(blobID))
	defer c.pc.sharedUnlock(string(blobID))

	// see if we have the full blob cached by extracting a partial range.
	if c.pc.GetPartial(ctx, BlobIDCacheKey(blobID), offset, length, output) {
		return nil
	}

	// acquire exclusive lock on the content
	c.pc.exclusiveLock(contentID)
	defer c.pc.exclusiveUnlock(contentID)

	output.Reset()

	if c.pc.GetFull(ctx, ContentIDCacheKey(contentID), output) {
		return nil
	}

	if err := c.st.GetBlob(ctx, blobID, offset, length, output); err != nil {
		c.pc.reportMissError()

		return errors.Wrapf(err, "failed to get blob with ID %s", blobID)
	}

	c.pc.reportMissBytes(int64(output.Length()))

	c.pc.Put(ctx, ContentIDCacheKey(contentID), output.Bytes())

	return nil
}

func (c *contentCacheImpl) Close(ctx context.Context) {
	c.pc.Close(ctx)
}

func (c *contentCacheImpl) PrefetchBlob(ctx context.Context, blobID blob.ID) error {
	var blobData gather.WriteBuffer
	defer blobData.Close()

	// see if it's already cached before taking a lock
	if c.pc.GetPartial(ctx, BlobIDCacheKey(blobID), 0, 1, &blobData) {
		return nil
	}

	// acquire exclusive lock for the blob.
	c.pc.exclusiveLock(string(blobID))
	defer c.pc.exclusiveUnlock(string(blobID))

	if c.pc.GetPartial(ctx, BlobIDCacheKey(blobID), 0, 1, &blobData) {
		return nil
	}

	return c.fetchBlobInternal(ctx, blobID, &blobData)
}

func (c *contentCacheImpl) CacheStorage() Storage {
	return c.pc.cacheStorage
}

// NewContentCache creates new content cache for data contents.
func NewContentCache(ctx context.Context, st blob.Storage, opt Options, mr *metrics.Registry) (ContentCache, error) {
	cacheStorage := opt.Storage
	if cacheStorage == nil {
		if opt.BaseCacheDirectory == "" {
			return passthroughContentCache{st}, nil
		}

		var err error

		cacheStorage, err = NewStorageOrNil(ctx, opt.BaseCacheDirectory, opt.Sweep.MaxSizeBytes, opt.CacheSubDir)
		if err != nil {
			return nil, errors.Wrap(err, "error initializing cache storage")
		}
	}

	pc, err := NewPersistentCache(ctx, opt.CacheSubDir, cacheStorage, cacheprot.ChecksumProtection(opt.HMACSecret), opt.Sweep, mr, opt.TimeNow)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheImpl{
		st:             st,
		pc:             pc,
		fetchFullBlobs: opt.FetchFullBlobs,
	}, nil
}
