package block

import (
	"sync"

	"github.com/kopia/kopia/storage"
)

type inMemoryCommittedBlockIndex struct {
	mu             sync.Mutex
	blocks         map[ContentID]Info
	physicalBlocks map[PhysicalBlockID]bool
}

func (b *inMemoryCommittedBlockIndex) getBlock(blockID ContentID) (Info, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	i, ok := b.blocks[blockID]
	if !ok {
		return Info{}, storage.ErrBlockNotFound
	}
	return i, nil
}

func (b *inMemoryCommittedBlockIndex) commit(indexBlockID PhysicalBlockID, infos map[ContentID]Info) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for k, i := range infos {
		b.blocks[k] = i
		delete(infos, k)
	}
}

func (b *inMemoryCommittedBlockIndex) load(indexBlockID PhysicalBlockID, indexes []packIndex) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.physicalBlocks[indexBlockID] {
		return 0, nil
	}

	var updated int
	for _, ndx := range indexes {
		_ = ndx.iterate(func(i Info) error {
			old, ok := b.blocks[i.BlockID]
			if !ok || old.TimestampNanos < i.TimestampNanos {
				b.blocks[i.BlockID] = i
				updated++
			}
			return nil
		})
	}

	b.physicalBlocks[indexBlockID] = true

	return updated, nil
}

func (b *inMemoryCommittedBlockIndex) listBlocks(prefix ContentID, cb func(i Info) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, v := range b.blocks {
		if err := cb(v); err != nil {
			return err
		}
	}

	return nil
}

func (b *inMemoryCommittedBlockIndex) hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.physicalBlocks[indexBlockID], nil
}
