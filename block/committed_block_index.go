package block

import (
	"github.com/kopia/kopia/internal/packindex"
)

type committedBlockIndex interface {
	getBlock(blockID string) (Info, error)
	listBlocks(prefix string, cb func(i Info) error) error

	addBlock(indexBlockID string, indexData []byte, use bool) error
	hasIndexBlockID(indexBlockID string) (bool, error)

	use(indexBlockIDs []string) error
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		physicalBlocks: make(map[string]packindex.Index),
	}
}
