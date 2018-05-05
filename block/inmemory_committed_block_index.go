package block

import (
	"bytes"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
	"github.com/kopia/kopia/storage"
)

type inMemoryCommittedBlockIndex struct {
	mu             sync.Mutex
	indexes        packindex.Merged
	physicalBlocks map[PhysicalBlockID]bool
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

func (b *inMemoryCommittedBlockIndex) addBlock(indexBlockID PhysicalBlockID, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ndx, err := packindex.Open(bytes.NewReader(data))
	if err != nil {
		return err
	}

	if !b.physicalBlocks[indexBlockID] {
		b.physicalBlocks[indexBlockID] = true
		b.indexes = append(b.indexes, ndx)
	}

	return nil
}

func (b *inMemoryCommittedBlockIndex) listBlocks(prefix string, cb func(i Info) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.indexes.Iterate(prefix, cb)
}

func (b *inMemoryCommittedBlockIndex) hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.physicalBlocks[indexBlockID], nil
}
