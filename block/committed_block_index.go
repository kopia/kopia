package block

import (
	"github.com/kopia/kopia/internal/packindex"
)

type committedBlockIndex interface {
	getBlock(blockID string) (Info, error)
	listBlocks(prefix string, cb func(i Info) error) error

	addBlock(indexBlockID PhysicalBlockID, indexData []byte, use bool) error
	hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error)

	use(indexBlockIDs []PhysicalBlockID) error
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		physicalBlocks: make(map[PhysicalBlockID]packindex.Index),
	}
}
