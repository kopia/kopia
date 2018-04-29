package block

import (
	"errors"

	"github.com/kopia/kopia/internal/blockmgrpb"
)

type packIndex interface {
	packBlockID() PhysicalBlockID
	packLength() uint32
	formatVersion() int32
	createTimeNanos() int64

	getBlock(blockID ContentID) (Info, error)
	iterate(func(info Info) error) error
	addToIndexes(pb *blockmgrpb.Indexes)
}

type packIndexBuilder interface {
	packIndex

	addInlineBlock(blockID ContentID, data []byte)
	addPackedBlock(blockID ContentID, offset, size uint32)
	clearInlineBlocks() map[ContentID][]byte
	deleteBlock(blockID ContentID)
	finishPack(packBlockID PhysicalBlockID, packLength uint32, formatVersion int32)
}

func packOffsetAndSize(offset uint32, size uint32) uint64 {
	return uint64(offset)<<32 | uint64(size)
}

func unpackOffsetAndSize(os uint64) (uint32, uint32) {
	offset := uint32(os >> 32)
	size := uint32(os)

	return offset, size
}

func copyPackIndex(dst packIndexBuilder, src packIndex) {
	_ = src.iterate(func(i Info) error {
		if i.Payload != nil {
			dst.addInlineBlock(i.BlockID, i.Payload)
			return nil
		}
		if i.Deleted {
			dst.deleteBlock(i.BlockID)
			return nil
		}

		dst.addPackedBlock(i.BlockID, i.PackOffset, i.Length)
		return nil
	})
	dst.finishPack(src.packBlockID(), src.packLength(), src.formatVersion())
}

func isIndexEmpty(ndx packIndex) bool {
	return nil == ndx.iterate(
		func(bi Info) error {
			return errors.New("have items")
		})
}
