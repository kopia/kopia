package block

import (
	"bytes"
	"encoding/hex"
	"sort"

	"github.com/kopia/kopia/internal/blockmgrpb"
	"github.com/kopia/kopia/storage"
)

var zeroBytes = []byte{}

type protoPackIndexV2 struct {
	ndx    *blockmgrpb.IndexV2
	sorted bool
}

var _ packIndex = protoPackIndexV2{nil, false}

func (p protoPackIndexV2) addToIndexes(pb *blockmgrpb.Indexes) {
	pb.IndexesV2 = append(pb.IndexesV2, p.ndx)
}

func (p protoPackIndexV2) createTimeNanos() int64 {
	return int64(p.ndx.CreateTimeNanos)
}

func (p protoPackIndexV2) formatVersion() int32 {
	return p.ndx.FormatVersion
}

func (p protoPackIndexV2) finishPack(packBlockID PhysicalBlockID, packLength uint32, formatVersion int32) {
	sort.Slice(p.ndx.Items, func(i, j int) bool {
		return bytes.Compare(p.ndx.Items[i].BlockId, p.ndx.Items[j].BlockId) < 0
	})
	p.sorted = true
	p.ndx.PackBlockId = string(packBlockID)
	p.ndx.PackLength = packLength
	p.ndx.FormatVersion = formatVersion
}

func (p protoPackIndexV2) clearInlineBlocks() map[ContentID][]byte {
	result := map[ContentID][]byte{}
	var remaining []*blockmgrpb.IndexV2_Item
	for _, i := range p.ndx.Items {
		if i.Payload != nil {
			result[bytesToContentID(i.BlockId)] = i.Payload
		} else {
			remaining = append(remaining, i)
		}
	}
	p.ndx.Items = remaining
	return result
}

func bytesToContentID(b []byte) ContentID {
	if len(b) == 0 {
		return ""
	}
	if b[0] == 0xff {
		return ContentID(b[1:])
	}
	prefix := ""
	if b[0] != 0 {
		prefix = string(b[0:1])
	}

	return ContentID(prefix + hex.EncodeToString(b[1:]))
}

func contentIDToBytes(c ContentID) []byte {
	var prefix []byte
	if len(c)%2 == 1 {
		prefix = []byte(c[0:1])
		c = c[1:]
	} else {
		prefix = []byte{0}
	}

	b, err := hex.DecodeString(string(c))
	if err != nil {
		return append([]byte{0xff}, []byte(c)...)
	}

	return append(prefix, b...)
}

func (p protoPackIndexV2) infoForPayload(blockID []byte, payload []byte) Info {
	if payload == nil {
		payload = zeroBytes
	}
	return Info{
		BlockID:        bytesToContentID(blockID),
		Length:         uint32(len(payload)),
		Payload:        payload,
		TimestampNanos: int64(p.ndx.CreateTimeNanos),
	}
}

func (p protoPackIndexV2) infoForOffsetAndSize(blockID []byte, os uint64) Info {
	offset, size := unpackOffsetAndSize(os)
	return Info{
		BlockID:        bytesToContentID(blockID),
		PackBlockID:    p.packBlockID(),
		PackOffset:     offset,
		Length:         size,
		TimestampNanos: int64(p.ndx.CreateTimeNanos),
		FormatVersion:  p.ndx.FormatVersion,
	}
}

func (p protoPackIndexV2) infoForDeletedBlock(blockID []byte) Info {
	return Info{
		BlockID:        bytesToContentID(blockID),
		Deleted:        true,
		TimestampNanos: int64(p.ndx.CreateTimeNanos),
	}
}

func (p protoPackIndexV2) findItem(blockID ContentID) *blockmgrpb.IndexV2_Item {
	b := contentIDToBytes(blockID)
	if p.sorted {
		result := sort.Search(len(p.ndx.Items), func(i int) bool {
			return bytes.Compare(p.ndx.Items[i].BlockId, b) >= 0
		})
		if result < len(p.ndx.Items) && blockID == bytesToContentID(p.ndx.Items[result].BlockId) {
			return p.ndx.Items[result]
		}
	} else {
		for _, it := range p.ndx.Items {
			if bytes.Equal(b, it.BlockId) {
				return it
			}
		}
	}
	return nil
}

func (p protoPackIndexV2) getBlock(blockID ContentID) (Info, error) {
	it := p.findItem(blockID)
	if it == nil {
		return Info{}, storage.ErrBlockNotFound
	}

	return p.infoForItem(it), nil
}

func (p protoPackIndexV2) infoForItem(it *blockmgrpb.IndexV2_Item) Info {
	if it.Deleted {
		return p.infoForDeletedBlock(it.BlockId)
	}
	if it.OffsetSize != 0 {
		return p.infoForOffsetAndSize(it.BlockId, it.OffsetSize)
	}

	return p.infoForPayload(it.BlockId, it.Payload)
}

func (p protoPackIndexV2) iterate(cb func(Info) error) error {
	for _, it := range p.ndx.Items {
		if err := cb(p.infoForItem(it)); err != nil {
			return err
		}
	}
	return nil
}

func (p protoPackIndexV2) packBlockID() PhysicalBlockID {
	return PhysicalBlockID(p.ndx.PackBlockId)
}

func (p protoPackIndexV2) packLength() uint32 {
	return p.ndx.PackLength
}

func (p protoPackIndexV2) addInlineBlock(blockID ContentID, data []byte) {
	p.ndx.Items = append(p.ndx.Items, &blockmgrpb.IndexV2_Item{
		BlockId: contentIDToBytes(blockID),
		Payload: append([]byte{}, data...),
	})
}

func (p protoPackIndexV2) addPackedBlock(blockID ContentID, offset, size uint32) {
	os := packOffsetAndSize(offset, size)
	p.ndx.Items = append(p.ndx.Items, &blockmgrpb.IndexV2_Item{
		BlockId:    contentIDToBytes(blockID),
		OffsetSize: os,
	})
}

func (p protoPackIndexV2) deleteBlock(blockID ContentID) {
	it := p.findItem(blockID)
	if it != nil {
		it.Deleted = true
		it.Payload = nil
		it.OffsetSize = 0
	} else {
		p.ndx.Items = append(p.ndx.Items, &blockmgrpb.IndexV2_Item{
			BlockId: contentIDToBytes(blockID),
			Deleted: true,
		})
	}
}

func newPackIndexV2(ts int64) packIndexBuilder {
	return protoPackIndexV2{&blockmgrpb.IndexV2{
		CreateTimeNanos: uint64(ts),
	}, false}
}
