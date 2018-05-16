package block

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/kopia/kopia/internal/packindex"
	"github.com/kopia/kopia/storage"
)

type inMemoryCommittedBlockIndex struct {
	mu             sync.Mutex
	indexes        packindex.Merged
	physicalBlocks map[PhysicalBlockID]packindex.Index
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

func (b *inMemoryCommittedBlockIndex) addBlock(indexBlockID PhysicalBlockID, data []byte, use bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.physicalBlocks[indexBlockID] == nil {
		ndx, err := packindex.Open(bytes.NewReader(data))
		if err != nil {
			return err
		}
		b.physicalBlocks[indexBlockID] = ndx

		if use {
			b.indexes = append(b.indexes, ndx)
		}
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

	return b.physicalBlocks[indexBlockID] != nil, nil
}

func (b *inMemoryCommittedBlockIndex) use(packBlockIDs []PhysicalBlockID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var newIndexes packindex.Merged
	defer func() {
		newIndexes.Close() //nolint:errcheck
	}()
	for _, e := range packBlockIDs {
		ndx := b.physicalBlocks[e]
		if ndx == nil {
			return fmt.Errorf("unable to open pack index %q", e)
		}

		//log.Printf("opened %v with %v entries", e, ndx.EntryCount())
		newIndexes = append(newIndexes, ndx)
	}
	b.indexes = newIndexes
	newIndexes = nil
	return nil
}
