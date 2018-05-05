package block

import (
	"github.com/kopia/kopia/internal/packindex"
)

type committedBlockIndex interface {
	getBlock(blockID string) (Info, error)
	commit(indexBlockID PhysicalBlockID, pendingBlocks packindex.Builder) error
	load(indexBlockID PhysicalBlockID, data []byte) (int, error)
	listBlocks(prefix string, cb func(i Info) error) error
	hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error)
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		blocks:         make(map[string]Info),
		physicalBlocks: make(map[PhysicalBlockID]bool),
	}
}
