package block

import (
	"github.com/kopia/kopia/storage"
)

type inMemoryCommittedBlockIndex struct {
	blocks map[ContentID]Info
}

func (b *inMemoryCommittedBlockIndex) getBlock(blockID ContentID) (Info, error) {
	i, ok := b.blocks[blockID]
	if !ok {
		return Info{}, storage.ErrBlockNotFound
	}
	return i, nil
}

func (b *inMemoryCommittedBlockIndex) commit(infos map[ContentID]Info) {
	for k, i := range infos {
		b.blocks[k] = i
		delete(infos, k)
	}
}

func (b *inMemoryCommittedBlockIndex) load(ndx packIndex) bool {
	var updated bool
	_ = ndx.iterate(func(i Info) error {
		old, ok := b.blocks[i.BlockID]
		if !ok || old.Timestamp.Before(i.Timestamp) {
			b.blocks[i.BlockID] = i
			updated = true
		}
		return nil
	})

	return updated
}

func (b *inMemoryCommittedBlockIndex) listBlocks(prefix ContentID, cb func(i Info) error) error {
	for _, v := range b.blocks {
		if err := cb(v); err != nil {
			return err
		}
	}

	return nil
}
