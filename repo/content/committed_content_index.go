package content

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// smallIndexEntryCountThreshold is the threshold to determine whether an
// index is small. Any index with fewer entries than this threshold
// will be combined in-memory to reduce the number of segments and speed up
// large index operations (such as verification of all contents).
const smallIndexEntryCountThreshold = 100

type committedContentIndex struct {
	rev   int64
	cache committedContentIndexCache

	mu     sync.Mutex
	inUse  map[blob.ID]packIndex
	merged mergedIndex

	v1PerContentOverhead uint32
	indexVersion         int

	// fetchOne loads one index blob
	fetchOne func(ctx context.Context, blobID blob.ID) ([]byte, error)

	log logging.Logger
}

type committedContentIndexCache interface {
	hasIndexBlobID(ctx context.Context, indexBlob blob.ID) (bool, error)
	addContentToCache(ctx context.Context, indexBlob blob.ID, data []byte) error
	openIndex(ctx context.Context, indexBlob blob.ID) (packIndex, error)
	expireUnused(ctx context.Context, used []blob.ID) error
}

func (c *committedContentIndex) revision() int64 {
	return atomic.LoadInt64(&c.rev)
}

func (c *committedContentIndex) getContent(contentID ID) (Info, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, err := c.merged.GetInfo(contentID)
	if info != nil {
		return info, nil
	}

	if err == nil {
		return nil, ErrContentNotFound
	}

	return nil, err
}

func (c *committedContentIndex) addContent(ctx context.Context, indexBlobID blob.ID, data []byte, use bool) error {
	atomic.AddInt64(&c.rev, 1)

	if err := c.cache.addContentToCache(ctx, indexBlobID, data); err != nil {
		return errors.Wrap(err, "error adding content to cache")
	}

	if !use {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.inUse[indexBlobID] != nil {
		return nil
	}

	ndx, err := c.cache.openIndex(ctx, indexBlobID)
	if err != nil {
		return errors.Wrapf(err, "unable to open pack index %q", indexBlobID)
	}

	c.inUse[indexBlobID] = ndx
	c.merged = append(c.merged, ndx)

	return nil
}

func (c *committedContentIndex) listContents(r IDRange, cb func(i Info) error) error {
	c.mu.Lock()
	m := append(mergedIndex(nil), c.merged...)
	c.mu.Unlock()

	return m.Iterate(r, cb)
}

func (c *committedContentIndex) packFilesChanged(packFiles []blob.ID) bool {
	if len(packFiles) != len(c.inUse) {
		return true
	}

	for _, packFile := range packFiles {
		if c.inUse[packFile] == nil {
			return true
		}
	}

	return false
}

func (c *committedContentIndex) merge(ctx context.Context, packFiles []blob.ID) (merged mergedIndex, used map[blob.ID]packIndex, finalErr error) {
	used = map[blob.ID]packIndex{}

	defer func() {
		// we failed along the way, close the merged index.
		if finalErr != nil {
			merged.Close() //nolint:errcheck
		}
	}()

	for _, e := range packFiles {
		ndx, err := c.cache.openIndex(ctx, e)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to open pack index %q", e)
		}

		merged = append(merged, ndx)
		used[e] = ndx
	}

	mergedAndCombined, err := c.combineSmallIndexes(merged)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to combine small indexes")
	}

	c.log.Debugf("combined %v into %v index segments", len(merged), len(mergedAndCombined))

	merged = mergedAndCombined

	return
}

// Uses packFiles for indexing. An error is returned if the
// indices cannot be read for any reason.
func (c *committedContentIndex) use(ctx context.Context, packFiles []blob.ID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.packFilesChanged(packFiles) {
		return nil
	}

	mergedAndCombined, newInUse, err := c.merge(ctx, packFiles)
	if err != nil {
		return err
	}

	atomic.AddInt64(&c.rev, 1)

	c.merged = mergedAndCombined
	c.inUse = newInUse

	if err := c.cache.expireUnused(ctx, packFiles); err != nil {
		c.log.Errorf("unable to expire unused content index files: %v", err)
	}

	return nil
}

func (c *committedContentIndex) combineSmallIndexes(m mergedIndex) (mergedIndex, error) {
	var toKeep, toMerge mergedIndex

	for _, ndx := range m {
		if ndx.ApproximateCount() < smallIndexEntryCountThreshold {
			toMerge = append(toMerge, ndx)
		} else {
			toKeep = append(toKeep, ndx)
		}
	}

	if len(toMerge) <= 1 {
		return m, nil
	}

	b := packIndexBuilder{}

	for _, ndx := range toMerge {
		if err := ndx.Iterate(AllIDs, func(i Info) error {
			b.Add(i)
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "unable to iterate index entries")
		}
	}

	var buf bytes.Buffer

	if err := b.Build(&buf, c.indexVersion); err != nil {
		return nil, errors.Wrap(err, "error building combined in-memory index")
	}

	combined, err := openPackIndex(bytes.NewReader(buf.Bytes()), c.v1PerContentOverhead)
	if err != nil {
		return nil, errors.Wrap(err, "error opening combined in-memory index")
	}

	return append(toKeep, combined), nil
}

func (c *committedContentIndex) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pi := range c.inUse {
		if err := pi.Close(); err != nil {
			return errors.Wrap(err, "unable to close index")
		}
	}

	return nil
}

func (c *committedContentIndex) fetchIndexBlobs(ctx context.Context, indexBlobs []blob.ID) error {
	ch, err := c.missingIndexBlobs(ctx, indexBlobs)
	if err != nil {
		return err
	}

	if len(ch) == 0 {
		return nil
	}

	c.log.Debugf("Downloading %v new index blobs...", len(indexBlobs))

	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < parallelFetches; i++ {
		eg.Go(func() error {
			for indexBlobID := range ch {
				data, err := c.fetchOne(ctx, indexBlobID)
				if err != nil {
					return errors.Wrapf(err, "error loading index blob %v", indexBlobID)
				}

				if err := c.addContent(ctx, indexBlobID, data, false); err != nil {
					return errors.Wrap(err, "unable to add to committed content cache")
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error downloading indexes")
	}

	c.log.Debugf("Index blobs downloaded.")

	return nil
}

// missingIndexBlobs returns a closed channel filled with blob IDs that are not in committedContents cache.
func (c *committedContentIndex) missingIndexBlobs(ctx context.Context, blobs []blob.ID) (<-chan blob.ID, error) {
	ch := make(chan blob.ID, len(blobs))
	defer close(ch)

	for _, id := range blobs {
		has, err := c.cache.hasIndexBlobID(ctx, id)
		if err != nil {
			return nil, errors.Wrapf(err, "error determining whether index blob %v has been downloaded", id)
		}

		if !has {
			ch <- id
		}
	}

	return ch, nil
}

func newCommittedContentIndex(caching *CachingOptions,
	v1PerContentOverhead uint32,
	indexVersion int,
	fetchOne func(ctx context.Context, blobID blob.ID) ([]byte, error),
	baseLog logging.Logger,
) *committedContentIndex {
	log := logging.WithPrefix("[committed-content-index] ", baseLog)

	var cache committedContentIndexCache

	if caching.CacheDirectory != "" {
		dirname := filepath.Join(caching.CacheDirectory, "indexes")
		cache = &diskCommittedContentIndexCache{dirname, clock.Now, v1PerContentOverhead, log}
	} else {
		cache = &memoryCommittedContentIndexCache{
			contents:             map[blob.ID]packIndex{},
			v1PerContentOverhead: v1PerContentOverhead,
		}
	}

	return &committedContentIndex{
		cache:                cache,
		inUse:                map[blob.ID]packIndex{},
		v1PerContentOverhead: v1PerContentOverhead,
		indexVersion:         indexVersion,
		fetchOne:             fetchOne,
		log:                  baseLog,
	}
}
