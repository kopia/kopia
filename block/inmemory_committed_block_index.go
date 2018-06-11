package block

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
)

type inMemoryCommittedBlockIndex struct {
	mu                   sync.Mutex
	indexes              packindex.Merged
	usedPhysicalBlocks   map[string]packindex.Index
	cachedPhysicalBlocks map[string]packindex.Index
}

func (b *inMemoryCommittedBlockIndex) getBlock(blockID string) (Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	i, err := b.indexes.GetInfo(blockID)
	if err != nil {
		return Info{}, err
	}

	if i == nil {
		return Info{}, storage.ErrBlockNotFound
	}

	return *i, nil

}

func (b *inMemoryCommittedBlockIndex) addBlock(indexBlockID string, data []byte, use bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.cachedPhysicalBlocks[indexBlockID] == nil {
		ndx, err := packindex.Open(bytes.NewReader(data))
		if err != nil {
			return err
		}
		b.cachedPhysicalBlocks[indexBlockID] = ndx
	}

	if use && b.usedPhysicalBlocks[indexBlockID] == nil {
		ndx := b.cachedPhysicalBlocks[indexBlockID]
		b.usedPhysicalBlocks[indexBlockID] = ndx
		b.indexes = append(b.indexes, ndx)
	}
	return nil
}

func (b *inMemoryCommittedBlockIndex) listBlocks(prefix string, cb func(i Info) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.indexes.Iterate(prefix, cb)
}

func (b *inMemoryCommittedBlockIndex) hasIndexBlockID(indexBlockID string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.cachedPhysicalBlocks[indexBlockID] != nil, nil
}

func (b *inMemoryCommittedBlockIndex) packFilesChanged(packFiles []string) bool {
	if len(packFiles) != len(b.usedPhysicalBlocks) {
		return true
	}

	for _, packFile := range packFiles {
		if b.usedPhysicalBlocks[packFile] == nil {
			return true
		}
	}

	return false
}

func (b *inMemoryCommittedBlockIndex) use(packFiles []string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.packFilesChanged(packFiles) {
		return false, nil
	}

	log.Printf("using %v", packFiles)

	var newIndexes packindex.Merged
	newUsedBlocks := map[string]packindex.Index{}
	defer func() {
		newIndexes.Close() //nolint:errcheck
	}()
	for _, e := range packFiles {
		ndx := b.cachedPhysicalBlocks[e]
		if ndx == nil {
			return false, fmt.Errorf("unable to open pack index %q", e)
		}

		//log.Printf("opened %v with %v entries", e, ndx.EntryCount())
		newIndexes = append(newIndexes, ndx)
		newUsedBlocks[e] = ndx
	}
	b.indexes = newIndexes
	b.usedPhysicalBlocks = newUsedBlocks
	newIndexes = nil
	return true, nil
}
