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
}

type committedContentIndexCache interface {
	hasIndexBlobID(ctx context.Context, indexBlob blob.ID) (bool, error)
	addContentToCache(ctx context.Context, indexBlob blob.ID, data []byte) error
	openIndex(ctx context.Context, indexBlob blob.ID) (packIndex, error)
	expireUnused(ctx context.Context, used []blob.ID) error
}

func (b *committedContentIndex) revision() int64 {
	return atomic.LoadInt64(&b.rev)
}

func (b *committedContentIndex) getContent(contentID ID) (Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	info, err := b.merged.GetInfo(contentID)
	if info != nil {
		return *info, nil
	}

	if err == nil {
		return Info{}, ErrContentNotFound
	}

	return Info{}, err
}

func (b *committedContentIndex) addContent(ctx context.Context, indexBlobID blob.ID, data []byte, use bool) error {
	atomic.AddInt64(&b.rev, 1)

	if err := b.cache.addContentToCache(ctx, indexBlobID, data); err != nil {
		return errors.Wrap(err, "error adding content to cache")
	}

	if !use {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.inUse[indexBlobID] != nil {
		return nil
	}

	ndx, err := b.cache.openIndex(ctx, indexBlobID)
	if err != nil {
		return errors.Wrapf(err, "unable to open pack index %q", indexBlobID)
	}

	b.inUse[indexBlobID] = ndx
	b.merged = append(b.merged, ndx)

	return nil
}

func (b *committedContentIndex) listContents(r IDRange, cb func(i Info) error) error {
	b.mu.Lock()
	m := append(mergedIndex(nil), b.merged...)
	b.mu.Unlock()

	return m.Iterate(r, cb)
}

func (b *committedContentIndex) packFilesChanged(packFiles []blob.ID) bool {
	if len(packFiles) != len(b.inUse) {
		return true
	}

	for _, packFile := range packFiles {
		if b.inUse[packFile] == nil {
			return true
		}
	}

	return false
}

// Uses packFiles for indexing and returns whether or not the set of index
// packs have changed compared to the previous set. An error is returned if the
// indices cannot be read for any reason.
func (b *committedContentIndex) use(ctx context.Context, packFiles []blob.ID) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.packFilesChanged(packFiles) {
		return false, nil
	}

	atomic.AddInt64(&b.rev, 1)

	var newMerged mergedIndex

	newInUse := map[blob.ID]packIndex{}

	defer func() {
		newMerged.Close() //nolint:errcheck
	}()

	for _, e := range packFiles {
		ndx, err := b.cache.openIndex(ctx, e)
		if err != nil {
			return false, errors.Wrapf(err, "unable to open pack index %q", e)
		}

		newMerged = append(newMerged, ndx)
		newInUse[e] = ndx
	}

	mergedAndCombined, err := combineSmallIndexes(newMerged)
	if err != nil {
		return false, errors.Wrap(err, "unable to combine small indexes")
	}

	log(ctx).Debugf("combined %v into %v index segments", len(newMerged), len(mergedAndCombined))

	b.merged = mergedAndCombined
	b.inUse = newInUse

	if err := b.cache.expireUnused(ctx, packFiles); err != nil {
		log(ctx).Errorf("unable to expire unused content index files: %v", err)
	}

	newMerged = nil // prevent closing newMerged indices

	return true, nil
}

func combineSmallIndexes(m mergedIndex) (mergedIndex, error) {
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

	combined, err := openPackIndex(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, errors.Wrap(err, "error opening combined in-memory index")
	}

	return append(toKeep, combined), nil
}

func (b *committedContentIndex) close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, pi := range b.inUse {
		if err := pi.Close(); err != nil {
			return errors.Wrap(err, "unable to close index")
		}
	}

	return nil
}

func newCommittedContentIndex(caching *CachingOptions) *committedContentIndex {
	var cache committedContentIndexCache

	if caching.CacheDirectory != "" {
		dirname := filepath.Join(caching.CacheDirectory, "indexes")
		cache = &diskCommittedContentIndexCache{dirname, clock.Now}
	} else {
		cache = &memoryCommittedContentIndexCache{
			contents: map[blob.ID]packIndex{},
		}
	}

	return &committedContentIndex{
		cache: cache,
		inUse: map[blob.ID]packIndex{},
	}
}
