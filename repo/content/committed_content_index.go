package content

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
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

// Uses packFiles for indexing and returns whether or not the set of index
// packs have changed compared to the previous set. An error is returned if the
// indices cannot be read for any reason.
func (c *committedContentIndex) use(ctx context.Context, packFiles []blob.ID) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.packFilesChanged(packFiles) {
		return false, nil
	}

	atomic.AddInt64(&c.rev, 1)

	var newMerged mergedIndex

	newInUse := map[blob.ID]packIndex{}

	defer func() {
		newMerged.Close() //nolint:errcheck
	}()

	for _, e := range packFiles {
		ndx, err := c.cache.openIndex(ctx, e)
		if err != nil {
			return false, errors.Wrapf(err, "unable to open pack index %q", e)
		}

		newMerged = append(newMerged, ndx)
		newInUse[e] = ndx
	}

	mergedAndCombined, err := c.combineSmallIndexes(newMerged)
	if err != nil {
		return false, errors.Wrap(err, "unable to combine small indexes")
	}

	log(ctx).Debugf("combined %v into %v index segments", len(newMerged), len(mergedAndCombined))

	c.merged = mergedAndCombined
	c.inUse = newInUse

	if err := c.cache.expireUnused(ctx, packFiles); err != nil {
		log(ctx).Errorf("unable to expire unused content index files: %v", err)
	}

	newMerged = nil // prevent closing newMerged indices

	return true, nil
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

	if err := b.Build(&buf); err != nil {
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

func newCommittedContentIndex(caching *CachingOptions, v1PerContentOverhead uint32) *committedContentIndex {
	var cache committedContentIndexCache

	if caching.CacheDirectory != "" {
		dirname := filepath.Join(caching.CacheDirectory, "indexes")
		cache = &diskCommittedContentIndexCache{dirname, clock.Now, v1PerContentOverhead}
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
	}
}
