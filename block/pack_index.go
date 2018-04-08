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
}

type protoPackIndex struct {
	ndx *blockmgrpb.Index
}

var _ packIndex = protoPackIndex{nil}

func (p protoPackIndex) createTimeNanos() uint64 {
	return p.ndx.CreateTimeNanos
}

func (p protoPackIndex) finishPack(packBlockID PhysicalBlockID, packLength uint64) {
	p.ndx.PackBlockId = string(packBlockID)
	p.ndx.PackLength = packLength
}

func (p protoPackIndex) packedToInline(packedData []byte) {
	for k, os := range p.ndx.Items {
		offset, size := unpackOffsetAndSize(os)
		p.ndx.InlineItems[k] = packedData[offset : offset+size]
	}

	p.ndx.Items = map[string]uint64{}
}

func (p protoPackIndex) getBlock(blockID ContentID) (offset, size uint32, payload []byte, ok bool) {
	if payload, ok := p.ndx.InlineItems[string(blockID)]; ok {
		return 0, uint32(len(payload)), payload, true
	}

	if os, ok := p.ndx.Items[string(blockID)]; ok {
		offset, size := unpackOffsetAndSize(os)
		return offset, size, nil, true
	}

	return 0, 0, nil, false
}

func (p protoPackIndex) deletedBlockIDs() []ContentID {
	var result []ContentID

	for _, d := range p.ndx.DeletedItems {
		result = append(result, ContentID(d))
	}
	return result
}

func (p protoPackIndex) packBlockID() PhysicalBlockID {
	return PhysicalBlockID(p.ndx.PackBlockId)
}

func (p protoPackIndex) packLength() uint64 {
	return p.ndx.PackLength
}

func (p protoPackIndex) addPackedBlock(blockID ContentID, offset, size uint32) {
	p.ndx.Items[string(blockID)] = packOffsetAndSize(offset, size)
}

func (p protoPackIndex) deleteBlock(blockID ContentID, addToDeleted bool) {
	delete(p.ndx.Items, string(blockID))
	delete(p.ndx.InlineItems, string(blockID))
	if addToDeleted {
		p.ndx.DeletedItems = append(p.ndx.DeletedItems, string(blockID))
	}
}

func (p protoPackIndex) activeBlockIDs() []ContentID {
	var result []ContentID
	for blkID := range p.ndx.Items {
		result = append(result, ContentID(blkID))
	}
	for blkID := range p.ndx.InlineItems {
		result = append(result, ContentID(blkID))
	}
	return result
}

func (p protoPackIndex) isEmpty() bool {
	return len(p.ndx.Items)+len(p.ndx.InlineItems)+len(p.ndx.DeletedItems) == 0
}

func loadPackIndexes(data []byte) ([]packIndex, error) {
	var b blockmgrpb.Indexes

	if err := proto.Unmarshal(data, &b); err != nil {
		return nil, err
	}

	var result []packIndex

	for _, ndx := range b.Indexes {
		result = append(result, protoPackIndex{ndx})
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
