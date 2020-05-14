package content

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

type committedContentIndex struct {
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
	if err := b.cache.addContentToCache(ctx, indexBlobID, data); err != nil {
		return err
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

	log(ctx).Debugf("set of index files has changed (had %v, now %v)", len(b.inUse), len(packFiles))

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

	b.merged = newMerged
	b.inUse = newInUse

	if err := b.cache.expireUnused(ctx, packFiles); err != nil {
		log(ctx).Warningf("unable to expire unused content index files: %v", err)
	}

	newMerged = nil // prevent closing newMerged indices

	return true, nil
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
		cache = &diskCommittedContentIndexCache{dirname}
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
