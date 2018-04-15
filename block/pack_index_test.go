package block

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestPackIndexV1(t *testing.T) {
	t.Run("new-v1", func(t *testing.T) {
		ts := time.Now()
		ndx := newPackIndexV1(ts)
		verifyPackIndex(t, ndx, ts)
	})
}

func verifyPackIndex(t *testing.T, ndx packIndexBuilder, ts time.Time) {
	if got, want := ndx.packBlockID(), PhysicalBlockID(""); got != want {
		t.Errorf("unexpected pack block ID: %q, wanted %q", got, want)
	}
	if got, want := ndx.packLength(), uint64(0); got != want {
		t.Errorf("unexpected pack length: %v, wanted %v", got, want)
	}

	if uint64(ts.UnixNano()) != ndx.createTimeNanos() {
		t.Errorf("unexpected created time: %v, wanted %v", ndx.createTimeNanos(), ts.UnixNano())
	}

	blockIDs := []ContentID{
		"xabcdef",
		"abcdef",
		"0123456780abcdef0123456780abcdef0123456780abcdef0123456780abcdef",
		"x0123456780abcdef0123456780abcdef0123456780abcdef0123456780abcdef",
	}

	var data []byte
	var offset uint32

	var blockData = map[ContentID][]byte{}
	var blockOffsets = map[ContentID]uint32{}
	var blockSizes = map[ContentID]uint32{}

	// add blocks to pack index
	for _, blockID := range blockIDs {
		blkSize := uint32(rand.Intn(100) + 1)
		blockContent := make([]byte, blkSize)
		blockData[blockID] = blockContent
		blockSizes[blockID] = blkSize
		blockOffsets[blockID] = offset
		data = append(data, blockContent...)

		verifyIndexBlockNotFound(t, ndx, blockID)
		ndx.addPackedBlock(blockID, offset, blkSize)
		verifyIndexBlockFoundPacked(t, ndx, blockID, offset, blkSize)
		offset += uint32(blkSize)
	}

	// verify offsets and sizes again
	for _, blockID := range blockIDs {
		verifyIndexBlockFoundPacked(t, ndx, blockID, blockOffsets[blockID], blockSizes[blockID])
	}

	ndx.packedToInline(data)
	for _, blockID := range blockIDs {
		verifyIndexBlockInline(t, ndx, blockID, blockData[blockID])
	}

	ndx.deleteBlock(blockIDs[0])
	verifyIndexBlockNotFound(t, ndx, blockIDs[0])
	verifyIndexBlockDeleted(t, ndx, blockIDs[0])

	ndx.deleteBlock(blockIDs[1])
	verifyIndexBlockNotFound(t, ndx, blockIDs[1])
	verifyIndexBlockDeleted(t, ndx, blockIDs[1])

	ndx.finishPack("some-physical-block", uint64(len(data)))
	if got, want := ndx.packBlockID(), PhysicalBlockID("some-physical-block"); got != want {
		t.Errorf("unexpected pack block ID: %q, wanted %q", got, want)
	}
	if got, want := ndx.packLength(), uint64(len(data)); got != want {
		t.Errorf("unexpected pack length: %v, wanted %v", got, want)
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
	bi, ok := ndx.getBlock(blockID)
	if !ok {
		t.Errorf("block %q not found in index", blockID)
	}

	if !bi.deleted {
		t.Errorf("expected block %q to be deleted", blockID)
	}
}

func verifyIndexBlockNotDeleted(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()

	bi, ok := ndx.getBlock(blockID)
	if !ok {
		t.Errorf("block %q not found in index", blockID)
	}

	if bi.deleted {
		t.Errorf("expected block %q to not be deleted", blockID)
	}
}

func verifyIndexBlockNotFound(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()
	bi, ok := ndx.getBlock(blockID)
	if ok && !bi.deleted {
		t.Errorf("block %q unexpectedly found", blockID)
	}
	if bi.payload != nil {
		t.Errorf("block %q unexpectedly has payload", blockID)
	}
	if bi.offset != 0 {
		t.Errorf("block %q unexpectedly has an offset", blockID)
	}
	if bi.size != 0 {
		t.Errorf("block %q unexpectedly has size", blockID)
	}
}

func verifyIndexBlockFoundPacked(t *testing.T, ndx packIndex, blockID ContentID, wantOffset, wantSize uint32) {
	t.Helper()
	bi, ok := ndx.getBlock(blockID)
	if !ok || bi.deleted {
		t.Errorf("block %q unexpectedly not found", blockID)
		return
	}
	if bi.payload != nil {
		t.Errorf("block %q unexpectedly has payload", blockID)
		return
	}
	if bi.offset != wantOffset {
		t.Errorf("block %q unexpectedly has an offset %v, wanted %v", blockID, bi.offset, wantOffset)
	}
	if bi.size != wantSize {
		t.Errorf("block %q unexpectedly has size %v, wanted %v", blockID, bi.size, wantSize)
	}
}

func verifyIndexBlockInline(t *testing.T, ndx packIndex, blockID ContentID, wantPayload []byte) {
	t.Helper()
	bi, ok := ndx.getBlock(blockID)
	if !ok || bi.deleted {
		t.Errorf("block %q unexpectedly not found", blockID)
		return
	}
	if !reflect.DeepEqual(bi.payload, wantPayload) {
		t.Errorf("block %q unexpectedly has payload %x, wanted %x", blockID, bi.payload, wantPayload)
		return
	}
	if bi.offset != 0 {
		t.Errorf("block %q unexpectedly has an offset %v, wanted %v", blockID, bi.offset, 0)
	}
	if bi.size != uint32(len(wantPayload)) {
		t.Errorf("block %q unexpectedly has a size %v, wanted %v", blockID, bi.size, len(wantPayload))
	}
}
