package block

import (
	"path/filepath"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

type committedBlockIndex struct {
	cache committedBlockIndexCache

	mu     sync.Mutex
	inUse  map[blob.ID]packIndex
	merged mergedIndex
}

type committedBlockIndexCache interface {
	hasIndexBlockID(indexBlob blob.ID) (bool, error)
	addBlockToCache(indexBlob blob.ID, data []byte) error
	openIndex(indexBlob blob.ID) (packIndex, error)
	expireUnused(used []blob.ID) error
}

func (b *committedBlockIndex) getBlock(blockID string) (Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	info, err := b.merged.GetInfo(blockID)
	if info != nil {
		return *info, nil
	}
	if err == nil {
		return Info{}, ErrBlockNotFound
	}
	return Info{}, err
}

func (b *committedBlockIndex) addBlock(indexBlockID blob.ID, data []byte, use bool) error {
	if err := b.cache.addBlockToCache(indexBlockID, data); err != nil {
		return err
	}

	if !use {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.inUse[indexBlockID] != nil {
		return nil
	}

	ndx, err := b.cache.openIndex(indexBlockID)
	if err != nil {
		return errors.Wrapf(err, "unable to open pack index %q", indexBlockID)
	}
	b.inUse[indexBlockID] = ndx
	b.merged = append(b.merged, ndx)
	return nil
}

func (b *committedBlockIndex) listBlocks(prefix string, cb func(i Info) error) error {
	b.mu.Lock()
	m := append(mergedIndex(nil), b.merged...)
	b.mu.Unlock()

	return m.Iterate(prefix, cb)
}

func (b *committedBlockIndex) packFilesChanged(packFiles []blob.ID) bool {
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

func (b *committedBlockIndex) use(packFiles []blob.ID) (bool, error) {
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
		log.Warningf("unable to expire unused block index files: %v", err)
	}
	newMerged = nil

	return true, nil
}

func newCommittedBlockIndex(caching CachingOptions) (*committedBlockIndex, error) {
	var cache committedBlockIndexCache

	if caching.CacheDirectory != "" {
		dirname := filepath.Join(caching.CacheDirectory, "indexes")
		cache = &diskCommittedBlockIndexCache{dirname}
	} else {
		cache = &memoryCommittedBlockIndexCache{
			blocks: map[blob.ID]packIndex{},
		}
	}

	return &committedBlockIndex{
		cache: cache,
		inUse: map[blob.ID]packIndex{},
	}, nil
}
