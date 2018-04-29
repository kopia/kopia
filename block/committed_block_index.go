package block

import (
	"github.com/kopia/kopia/internal/packindex"
)

type committedBlockIndex interface {
	getBlock(blockID ContentID) (Info, error)
	commit(indexBlockID PhysicalBlockID, pendingBlocks packindex.Builder) error
	load(indexBlockID PhysicalBlockID, data []byte) (int, error)
	listBlocks(prefix ContentID, cb func(i Info) error) error
	hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error)
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		blocks:         make(map[ContentID]Info),
		physicalBlocks: make(map[PhysicalBlockID]bool),
	}
}
