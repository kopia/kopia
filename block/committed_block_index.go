package block

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
	"github.com/kopia/kopia/storage"
)

type committedBlockIndex struct {
	cache committedBlockIndexCache

	mu     sync.Mutex
	inUse  map[string]packindex.Index
	merged packindex.Merged
}

type committedBlockIndexCache interface {
	hasIndexBlockID(indexBlockID string) (bool, error)
	addBlockToCache(indexBlockID string, data []byte) error
	openIndex(indexBlockID string) (packindex.Index, error)
	expireUnused(used []string) error
}

func (b *committedBlockIndex) getBlock(blockID string) (Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	info, err := b.merged.GetInfo(blockID)
	if info != nil {
		return *info, nil
	}
	if err == nil {
		return Info{}, storage.ErrBlockNotFound
	}
	return Info{}, err
}

func (b *committedBlockIndex) addBlock(indexBlockID string, data []byte, use bool) error {
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
		return fmt.Errorf("unable to open pack index %q: %v", indexBlockID, err)
	}
	b.inUse[indexBlockID] = ndx
	b.merged = append(b.merged, ndx)
	return nil
}

func (b *committedBlockIndex) listBlocks(prefix string, cb func(i Info) error) error {
	b.mu.Lock()
	m := b.merged
	b.mu.Unlock()

	return m.Iterate(prefix, cb)
}

func (b *committedBlockIndex) packFilesChanged(packFiles []string) bool {
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

func (b *committedBlockIndex) use(packFiles []string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.packFilesChanged(packFiles) {
		return false, nil
	}
	log.Debugf("set of index files has changed (had %v, now %v)", len(b.inUse), len(packFiles))

	var newMerged packindex.Merged
	newInUse := map[string]packindex.Index{}
	defer func() {
		newMerged.Close() //nolint:errcheck
	}()

	for _, e := range packFiles {
		ndx, err := b.cache.openIndex(e)
		if err != nil {
			return false, fmt.Errorf("unable to open pack index %q: %v", e, err)
		}

		log.Debugf("opened %v with %v entries", e, ndx.EntryCount())
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
			blocks: map[string]packindex.Index{},
		}
	}

	return &committedBlockIndex{
		cache: cache,
		inUse: map[string]packindex.Index{},
	}, nil
}
