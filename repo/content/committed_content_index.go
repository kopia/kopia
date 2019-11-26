package content

import (
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
	hasIndexBlobID(indexBlob blob.ID) (bool, error)
	addContentToCache(indexBlob blob.ID, data []byte) error
	openIndex(indexBlob blob.ID) (packIndex, error)
	expireUnused(used []blob.ID) error
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

func (b *committedContentIndex) addContent(indexBlobID blob.ID, data []byte, use bool) error {
	if err := b.cache.addContentToCache(indexBlobID, data); err != nil {
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

	ndx, err := b.cache.openIndex(indexBlobID)
	if err != nil {
		return errors.Wrapf(err, "unable to open pack index %q", indexBlobID)
	}

	b.inUse[indexBlobID] = ndx
	b.merged = append(b.merged, ndx)

	return nil
}

func (b *committedContentIndex) listContents(prefix ID, cb func(i Info) error) error {
	b.mu.Lock()
	m := append(mergedIndex(nil), b.merged...)
	b.mu.Unlock()

	return m.Iterate(prefix, cb)
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

func (b *committedContentIndex) use(packFiles []blob.ID) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.packFilesChanged(packFiles) {
		return false, nil
	}

	log.Debugf("set of index files has changed (had %v, now %v)", len(b.inUse), len(packFiles))

	var newMerged mergedIndex

	newInUse := map[blob.ID]packIndex{}

	defer func() {
		newMerged.Close() //nolint:errcheck
	}()

	for _, e := range packFiles {
		ndx, err := b.cache.openIndex(e)
		if err != nil {
			return false, errors.Wrapf(err, "unable to open pack index %q", e)
		}

		newMerged = append(newMerged, ndx)
		newInUse[e] = ndx
	}

	b.merged = newMerged
	b.inUse = newInUse

	if err := b.cache.expireUnused(packFiles); err != nil {
		log.Warningf("unable to expire unused content index files: %v", err)
	}

	newMerged = nil

	return true, nil
}

func newCommittedContentIndex(caching CachingOptions) *committedContentIndex {
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
