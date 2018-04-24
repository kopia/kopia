package block

import (
	"time"

	"github.com/kopia/kopia/internal/blockmgrpb"
	"github.com/kopia/kopia/storage"
)

type protoPackIndexV1 struct {
	ndx *blockmgrpb.IndexV1
}

var _ packIndex = protoPackIndexV1{nil}

func (p protoPackIndexV1) addToIndexes(pb *blockmgrpb.Indexes) {
	pb.IndexesV1 = append(pb.IndexesV1, p.ndx)
}

func (p protoPackIndexV1) createTimeNanos() int64 {
	return int64(p.ndx.CreateTimeNanos)
}

func (p protoPackIndexV1) formatVersion() int32 {
	return p.ndx.FormatVersion
}

func (p protoPackIndexV1) finishPack(packBlockID PhysicalBlockID, packLength uint32, formatVersion int32) {
	p.ndx.PackBlockId = string(packBlockID)
	p.ndx.PackLength = packLength
	p.ndx.FormatVersion = formatVersion
}

func (p protoPackIndexV1) clearInlineBlocks() map[ContentID][]byte {
	result := map[ContentID][]byte{}
	for k, b := range p.ndx.InlineItems {
		result[ContentID(k)] = b
	}
	p.ndx.InlineItems = map[string][]byte{}
	return result
}

func (p protoPackIndexV1) infoForPayload(blockID ContentID, payload []byte) Info {
	return Info{
		BlockID:   blockID,
		Length:    uint32(len(payload)),
		Payload:   payload,
		Timestamp: time.Unix(0, int64(p.ndx.CreateTimeNanos)),
	}
}

func (p protoPackIndexV1) infoForOffsetAndSize(blockID ContentID, os uint64) Info {
	offset, size := unpackOffsetAndSize(os)
	return Info{
		BlockID:       blockID,
		PackBlockID:   p.packBlockID(),
		PackOffset:    offset,
		Length:        size,
		Timestamp:     time.Unix(0, int64(p.ndx.CreateTimeNanos)),
		FormatVersion: p.ndx.FormatVersion,
	}
}

func (p protoPackIndexV1) infoForDeletedBlock(blockID ContentID) Info {
	return Info{
		BlockID:   blockID,
		Deleted:   true,
		Timestamp: time.Unix(0, int64(p.ndx.CreateTimeNanos)),
	}
}

func (p protoPackIndexV1) getBlock(blockID ContentID) (Info, error) {
	if payload, ok := p.ndx.InlineItems[string(blockID)]; ok {
		return p.infoForPayload(blockID, payload), nil
	}

	if offsetAndSize, ok := p.ndx.Items[string(blockID)]; ok {
		return p.infoForOffsetAndSize(blockID, offsetAndSize), nil
	}

	for _, del := range p.ndx.DeletedItems {
		if del == string(blockID) {
			return p.infoForDeletedBlock(blockID), nil
		}
	}

	return Info{}, storage.ErrBlockNotFound
}

func (p protoPackIndexV1) iterate(cb func(Info) error) error {
	for blockID, offsetAndSize := range p.ndx.Items {
		if err := cb(p.infoForOffsetAndSize(ContentID(blockID), offsetAndSize)); err != nil {
			return err
		}
	}
	for blockID, payload := range p.ndx.InlineItems {
		if err := cb(p.infoForPayload(ContentID(blockID), payload)); err != nil {
			return err
		}
	}
	for _, blockID := range p.ndx.DeletedItems {
		if err := cb(p.infoForDeletedBlock(ContentID(blockID))); err != nil {
			return err
		}
	}

	return nil
}

func (p protoPackIndexV1) packBlockID() PhysicalBlockID {
	return PhysicalBlockID(p.ndx.PackBlockId)
}

func (p protoPackIndexV1) packLength() uint32 {
	return p.ndx.PackLength
}

func (p protoPackIndexV1) addInlineBlock(blockID ContentID, data []byte) {
	p.ndx.InlineItems[string(blockID)] = append([]byte{}, data...)
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
