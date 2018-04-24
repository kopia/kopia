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
	if got, want := ndx.packLength(), uint32(0); got != want {
		t.Errorf("unexpected pack length: %v, wanted %v", got, want)
	}

	if ts.UnixNano() != ndx.createTimeNanos() {
		t.Errorf("unexpected created time: %v, wanted %v", ndx.createTimeNanos(), ts.UnixNano())
	}

	packedBlockIDs := []ContentID{
		"xabcdef",
		"abcdef",
		"0123456780abcdef0123456780abcdef0123456780abcdef0123456780abcdef",
		"x0123456780abcdef0123456780abcdef0123456780abcdef0123456780abcdef",
	}

	inlineBlockIDs := []ContentID{
		"xabcdefaa",
		"abcdefaa",
		"0123456780abcdef0123456780abcdef0123456780abcdef0123456780abcdefaa",
		"x0123456780abcdef0123456780abcdef0123456780abcdef0123456780abcdefaa",
	}

	var offset uint32

	var blockData = map[ContentID][]byte{}
	var blockOffsets = map[ContentID]uint32{}
	var blockSizes = map[ContentID]uint32{}

	// add blocks to pack index
	for _, blockID := range packedBlockIDs {
		blkSize := uint32(rand.Intn(100) + 1)
		blockSizes[blockID] = blkSize
		blockOffsets[blockID] = offset

		verifyIndexBlockNotFound(t, ndx, blockID)
		ndx.addPackedBlock(blockID, offset, blkSize)
		verifyIndexBlockFoundPacked(t, ndx, blockID, offset, blkSize)
		offset += uint32(blkSize)
	}

	// add some inline blocks to pack index
	for _, blockID := range inlineBlockIDs {
		blkSize := uint32(rand.Intn(100) + 1)
		blockContent := make([]byte, blkSize)
		rand.Read(blockContent)
		blockData[blockID] = blockContent

		verifyIndexBlockNotFound(t, ndx, blockID)
		ndx.addInlineBlock(blockID, blockContent)
		verifyIndexBlockInline(t, ndx, blockID, blockContent)
	}

	// verify offsets and sizes again
	for _, blockID := range packedBlockIDs {
		verifyIndexBlockFoundPacked(t, ndx, blockID, blockOffsets[blockID], blockSizes[blockID])
	}

	for _, blockID := range inlineBlockIDs {
		verifyIndexBlockInline(t, ndx, blockID, blockData[blockID])
	}

	ndx.deleteBlock(packedBlockIDs[0])
	verifyIndexBlockNotFound(t, ndx, packedBlockIDs[0])
	verifyIndexBlockDeleted(t, ndx, packedBlockIDs[0])

	inline := ndx.clearInlineBlocks()
	if !reflect.DeepEqual(inline, blockData) {
		t.Errorf("unexpected result of clearInlineBlocks(): %v, wanted %v", inline, blockData)
	}

	for k := range inline {
		verifyIndexBlockNotFound(t, ndx, k)
	}

	someLength := uint32(1234)

	ndx.finishPack("some-physical-block", someLength, 77)
	if got, want := ndx.packBlockID(), PhysicalBlockID("some-physical-block"); got != want {
		t.Errorf("unexpected pack block ID: %q, wanted %q", got, want)
	}
	if got, want := ndx.packLength(), someLength; got != want {
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
