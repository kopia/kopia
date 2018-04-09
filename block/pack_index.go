package block

import (
	"github.com/golang/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"
)

type packIndex interface {
	packBlockID() PhysicalBlockID
	packLength() uint64
	createTimeNanos() uint64

	addPackedBlock(blockID ContentID, offset, size uint32)
	getBlock(blockID ContentID) (offset, size uint32, payload []byte, ok bool)
	deleteBlock(blockID ContentID, addToDeleted bool)
	isEmpty() bool
	activeBlockIDs() []ContentID
	deletedBlockIDs() []ContentID

	packedToInline(data []byte)
	finishPack(packBlockID PhysicalBlockID, packLength uint64)
	addToIndexes(pb *blockmgrpb.Indexes)
}

func loadPackIndexes(data []byte) ([]packIndex, error) {
	var b blockmgrpb.Indexes

	if err := proto.Unmarshal(data, &b); err != nil {
		return nil, err
	}

	var result []packIndex

	for _, ndx := range b.IndexesV1 {
		result = append(result, protoPackIndexV1{ndx})
	}

	return result, nil
}

func packOffsetAndSize(offset uint32, size uint32) uint64 {
	return uint64(offset)<<32 | uint64(size)
}

func unpackOffsetAndSize(os uint64) (uint32, uint32) {
	offset := uint32(os >> 32)
	size := uint32(os)

	return offset, size
}
