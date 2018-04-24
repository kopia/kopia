package block

type committedBlockIndex interface {
	getBlock(blockID ContentID) (Info, error)
	commit(pendingBlocks map[ContentID]Info)
	load(ndx packIndex) bool
	listBlocks(prefix ContentID, cb func(i Info) error) error
}

func newCommittedBlockIndex() committedBlockIndex {
	return &inMemoryCommittedBlockIndex{
		blocks: make(map[ContentID]Info),
	}
}
