package block

type committedBlockIndex interface {
	getBlock(blockID string) (Info, error)
	listBlocks(prefix string, cb func(i Info) error) error

	addBlock(indexBlockID PhysicalBlockID, indexData []byte) error
	hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error)
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		physicalBlocks: make(map[PhysicalBlockID]bool),
	}
}
