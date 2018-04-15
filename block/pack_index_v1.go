package block

import (
	"time"

	"github.com/kopia/kopia/internal/blockmgrpb"
)

type protoPackIndexV1 struct {
	ndx *blockmgrpb.IndexV1
}

var _ packIndex = protoPackIndexV1{nil}

func (p protoPackIndexV1) addToIndexes(pb *blockmgrpb.Indexes) {
	pb.IndexesV1 = append(pb.IndexesV1, p.ndx)
}

func (p protoPackIndexV1) createTimeNanos() uint64 {
	return p.ndx.CreateTimeNanos
}

func (p protoPackIndexV1) finishPack(packBlockID PhysicalBlockID, packLength uint64) {
	p.ndx.PackBlockId = string(packBlockID)
	p.ndx.PackLength = packLength
}

func (p protoPackIndexV1) packedToInline(packedData []byte) {
	for k, os := range p.ndx.Items {
		offset, size := unpackOffsetAndSize(os)
		p.ndx.InlineItems[k] = packedData[offset : offset+size]
	}

	p.ndx.Items = map[string]uint64{}
}

func (p protoPackIndexV1) getBlock(blockID ContentID) (packBlockInfo, bool) {
	if payload, ok := p.ndx.InlineItems[string(blockID)]; ok {
		return packBlockInfo{size: uint32(len(payload)), payload: payload}, true
	}

	if os, ok := p.ndx.Items[string(blockID)]; ok {
		offset, size := unpackOffsetAndSize(os)
		return packBlockInfo{offset: offset, size: size}, true
	}

	for _, del := range p.ndx.DeletedItems {
		if del == string(blockID) {
			return packBlockInfo{deleted: true}, true
		}
	}

	return packBlockInfo{}, false
}

func (p protoPackIndexV1) iterate(cb func(ContentID, packBlockInfo) error) error {
	for k, v := range p.ndx.Items {
		offset, size := unpackOffsetAndSize(v)
		if err := cb(ContentID(k), packBlockInfo{offset: offset, size: size}); err != nil {
			return err
		}
	}
	for k, v := range p.ndx.InlineItems {
		if err := cb(ContentID(k), packBlockInfo{size: uint32(len(v)), payload: v}); err != nil {
			return err
		}
	}
	for _, k := range p.ndx.DeletedItems {
		if err := cb(ContentID(k), packBlockInfo{deleted: true}); err != nil {
			return err
		}
	}

	return nil
}

func (p protoPackIndexV1) packBlockID() PhysicalBlockID {
	return PhysicalBlockID(p.ndx.PackBlockId)
}

func (p protoPackIndexV1) packLength() uint64 {
	return p.ndx.PackLength
}

func (p protoPackIndexV1) addPackedBlock(blockID ContentID, offset, size uint32) {
	p.ndx.Items[string(blockID)] = packOffsetAndSize(offset, size)
}

func (p protoPackIndexV1) deleteBlock(blockID ContentID) {
	delete(p.ndx.Items, string(blockID))
	delete(p.ndx.InlineItems, string(blockID))
	p.ndx.DeletedItems = append(p.ndx.DeletedItems, string(blockID))
}

func newPackIndexV1(t time.Time) packIndexBuilder {
	return protoPackIndexV1{&blockmgrpb.IndexV1{
		Items:           make(map[string]uint64),
		InlineItems:     make(map[string][]byte),
		CreateTimeNanos: uint64(t.UnixNano()),
	}}
}
