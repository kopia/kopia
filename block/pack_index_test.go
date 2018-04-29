package block

import (
	"crypto/sha1"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blockmgrpb"
)

var (
	fakePackLength uint32 = 1234
)

func packIndexV1FromIndexes(p *blockmgrpb.Indexes) packIndex {
	return protoPackIndexV1{p.IndexesV1[0]}
}
func packIndexV2FromIndexes(p *blockmgrpb.Indexes) packIndex {
	return protoPackIndexV2{p.IndexesV2[0], true}
}

func TestPackIndexes(t *testing.T) {
	cases := []struct {
		name        string
		createNew   func(t int64) packIndexBuilder
		fromIndexes func(p *blockmgrpb.Indexes) packIndex
	}{
		{name: "v1", createNew: newPackIndexV1, fromIndexes: packIndexV1FromIndexes},
		{name: "v2", createNew: newPackIndexV2, fromIndexes: packIndexV2FromIndexes},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := time.Now()
			ndx := tc.createNew(ts.UnixNano())
			mdl := newTestModel()
			verifyPackIndexBuilder(t, ndx, ts, mdl)

			// now serialize and deserialize the pack
			var pb blockmgrpb.Indexes
			ndx.addToIndexes(&pb)
			ndx2 := tc.fromIndexes(&pb)
			mdl.verify(t, ndx2)
		})
	}
}

func verifyPackIndexBuilder(
	t *testing.T,
	ndx packIndexBuilder,
	ts time.Time,
	mdl *model,
) {
	if got, want := ndx.packBlockID(), PhysicalBlockID(""); got != want {
		t.Errorf("unexpected pack block ID: %q, wanted %q", got, want)
	}
	if got, want := ndx.packLength(), uint32(0); got != want {
		t.Errorf("unexpected pack length: %v, wanted %v", got, want)
	}

	if ts.UnixNano() != ndx.createTimeNanos() {
		t.Errorf("unexpected created time: %v, wanted %v", ndx.createTimeNanos(), ts.UnixNano())
	}

	var offset uint32
	blockNumber := 0

	randomBlockID := func() ContentID {
		h := sha1.New()
		fmt.Fprintf(h, "%v", blockNumber)
		blockNumber++
		return ContentID(fmt.Sprintf("%x", h.Sum(nil)))
	}

	// add blocks to pack index
	for i := 0; i < 100; i++ {
		blockID := randomBlockID()
		blkSize := uint32(rand.Intn(100) + 1)
		mdl.addPackedBlock(blockID, offset, blkSize)
		ndx.addPackedBlock(blockID, offset, blkSize)
		mdl.verify(t, ndx)
		offset += uint32(blkSize)
	}

	// add some inline blocks to pack index
	for i := 0; i < 100; i++ {
		blockID := randomBlockID()
		blkSize := uint32(rand.Intn(100) + 1)
		blockContent := make([]byte, blkSize)
		rand.Read(blockContent)
		mdl.addInlineBlock(blockID, blockContent)
		ndx.addInlineBlock(blockID, blockContent)
		mdl.verify(t, ndx)
	}

	// add zero-length block
	zeroLengthInlineBlockID := randomBlockID()
	mdl.addInlineBlock(zeroLengthInlineBlockID, []byte{})
	ndx.addInlineBlock(zeroLengthInlineBlockID, nil)

	// add zero-length packed block
	zeroLengthPackedBlockID := randomBlockID()
	mdl.addPackedBlock(zeroLengthPackedBlockID, 100, 0)
	ndx.addPackedBlock(zeroLengthPackedBlockID, 100, 0)

	// add some deleted blocks.
	for i := 0; i < 100; i++ {
		blockID := randomBlockID()
		mdl.deleteBlock(blockID)
		ndx.deleteBlock(blockID)
		mdl.verify(t, ndx)
	}

	cnt := 0
	for blockID := range mdl.blockData {
		ndx.deleteBlock(blockID)
		mdl.deleteBlock(blockID)
		mdl.verify(t, ndx)
		cnt++
		if cnt >= 5 {
			break
		}
	}
	cnt = 0
	for blockID := range mdl.blockSizes {
		ndx.deleteBlock(blockID)
		mdl.deleteBlock(blockID)
		mdl.verify(t, ndx)
		cnt++
		if cnt >= 5 {
			break
		}
	}

	ndx.finishPack("some-physical-block", fakePackLength, 77)
	if got, want := ndx.packBlockID(), PhysicalBlockID("some-physical-block"); got != want {
		t.Errorf("unexpected pack block ID: %q, wanted %q", got, want)
	}
	if got, want := ndx.packLength(), fakePackLength; got != want {
		t.Errorf("unexpected pack length: %v, wanted %v", got, want)
	}
	if got, want := ndx.formatVersion(), int32(77); got != want {
		t.Errorf("unexpected format version: %v, wanted %v", got, want)
	}
}

func blockIDSlicesEqual(x, y []ContentID) bool {
	xMap := make(map[ContentID]int)
	yMap := make(map[ContentID]int)

	for _, xElem := range x {
		xMap[xElem]++
	}
	for _, yElem := range y {
		yMap[yElem]++
	}

	for xMapKey, xMapVal := range xMap {
		if yMap[xMapKey] != xMapVal {
			return false
		}
	}

	return true
}

func verifyIndexBlockDeleted(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()

	verifyIndexBlockNotFound(t, ndx, blockID)
	bi, err := ndx.getBlock(blockID)
	if err != nil {
		t.Errorf("block %q not found in index", blockID)
	}

	if !bi.Deleted {
		t.Errorf("expected block %q to be deleted", blockID)
	}
}

func verifyIndexBlockNotDeleted(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()

	bi, err := ndx.getBlock(blockID)
	if err != nil {
		t.Errorf("block %q not found in index", blockID)
	}

	if bi.Deleted {
		t.Errorf("expected block %q to not be deleted", blockID)
	}
}

func verifyIndexBlockNotFound(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()
	bi, err := ndx.getBlock(blockID)
	if err == nil && !bi.Deleted {
		t.Errorf("block %q unexpectedly found", blockID)
	}
	if bi.Payload != nil {
		t.Errorf("block %q unexpectedly has payload", blockID)
	}
	if bi.PackOffset != 0 {
		t.Errorf("block %q unexpectedly has an offset", blockID)
	}
	if bi.Length != 0 {
		t.Errorf("block %q unexpectedly has size", blockID)
	}
}

func verifyIndexBlockFoundPacked(t *testing.T, ndx packIndex, blockID ContentID, wantOffset, wantSize uint32) {
	t.Helper()
	bi, err := ndx.getBlock(blockID)
	if err != nil || bi.Deleted {
		t.Errorf("block %q unexpectedly not found", blockID)
		return
	}
	if bi.Payload != nil {
		t.Errorf("block %q unexpectedly has payload", blockID)
		return
	}
	if bi.PackOffset != wantOffset {
		t.Errorf("block %q unexpectedly has an offset %v, wanted %v", blockID, bi.PackOffset, wantOffset)
	}
	if bi.Length != wantSize {
		t.Errorf("block %q unexpectedly has size %v, wanted %v", blockID, bi.Length, wantSize)
	}
}

func verifyIndexBlockInline(t *testing.T, ndx packIndex, blockID ContentID, wantPayload []byte) {
	t.Helper()
	bi, err := ndx.getBlock(blockID)
	if err != nil || bi.Deleted {
		t.Errorf("block %q unexpectedly not found", blockID)
		return
	}
	if !reflect.DeepEqual(bi.Payload, wantPayload) {
		t.Errorf("block %q unexpectedly has payload %x, wanted %x", blockID, bi.Payload, wantPayload)
		return
	}
	if bi.PackOffset != 0 {
		t.Errorf("block %q unexpectedly has an offset %v, wanted %v", blockID, bi.PackOffset, 0)
	}
	if bi.Length != uint32(len(wantPayload)) {
		t.Errorf("block %q unexpectedly has a size %v, wanted %v", blockID, bi.Length, len(wantPayload))
	}
}

type model struct {
	blockData     map[ContentID][]byte
	blockOffsets  map[ContentID]uint32
	blockSizes    map[ContentID]uint32
	deletedBlocks map[ContentID]bool
}

func (m *model) addPackedBlock(blockID ContentID, offset, size uint32) {
	m.blockSizes[blockID] = size
	m.blockOffsets[blockID] = offset
}

func (m *model) addInlineBlock(blockID ContentID, payload []byte) {
	m.blockData[blockID] = payload
}

func (m *model) deleteBlock(blockID ContentID) {
	delete(m.blockData, blockID)
	delete(m.blockOffsets, blockID)
	delete(m.blockSizes, blockID)
	m.deletedBlocks[blockID] = true
}

func (m *model) verify(t *testing.T, ndx packIndex) {
	t.Helper()
	for blockID := range m.blockOffsets {
		verifyIndexBlockFoundPacked(t, ndx, blockID, m.blockOffsets[blockID], m.blockSizes[blockID])
	}

	for blockID := range m.blockData {
		verifyIndexBlockInline(t, ndx, blockID, m.blockData[blockID])
	}

	for blockID := range m.deletedBlocks {
		verifyIndexBlockDeleted(t, ndx, blockID)
	}

	cnt := 0
	ndx.iterate(func(i Info) error {
		cnt++
		if i.Payload != nil {
			if m.blockData[i.BlockID] == nil {
				t.Errorf("unexpected inline block found: %v", i.BlockID)
			}
			return nil
		}
		if i.Deleted {
			if !m.deletedBlocks[i.BlockID] {
				t.Errorf("unexpected deleted block found: %v", i.BlockID)
			}
			return nil
		}
		if i.Length > 0 {
			if m.blockSizes[i.BlockID] == 0 {
				t.Errorf("unexpected packed block found: %v", i.BlockID)
			}
			return nil
		}
		return nil
	})

	if got, want := cnt, len(m.blockData)+len(m.deletedBlocks)+len(m.blockSizes); got != want {
		t.Errorf("unexpected number of items returned by iterate() %v, wanted %v", got, want)
	}
}

func newTestModel() *model {
	return &model{
		blockData:     map[ContentID][]byte{},
		blockOffsets:  map[ContentID]uint32{},
		blockSizes:    map[ContentID]uint32{},
		deletedBlocks: map[ContentID]bool{},
	}
}
