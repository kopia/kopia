package block

type committedBlockIndex interface {
	getBlock(blockID ContentID) (Info, error)
	commit(indexBlockID PhysicalBlockID, pendingBlocks map[ContentID]Info)
	load(indexBlockID PhysicalBlockID, indexes []packIndex) (int, error)
	listBlocks(prefix ContentID, cb func(i Info) error) error
	hasIndexBlockID(indexBlockID PhysicalBlockID) (bool, error)
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		blocks:         make(map[ContentID]Info),
		physicalBlocks: make(map[PhysicalBlockID]bool),
	}
}
