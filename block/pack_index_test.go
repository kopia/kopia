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
	if got, want := ndx.isEmpty(), true; got != want {
		t.Errorf("unexpected isEmpty(): %v, wanted %v", got, want)
	}
	if got, want := len(ndx.activeBlockIDs()), 0; got != want {
		t.Errorf("unexpected number of active block IDs: %v, wanted %v", got, want)
	}
	if got, want := len(ndx.deletedBlockIDs()), 0; got != want {
		t.Errorf("unexpected number of active block IDs: %v, wanted %v", got, want)
	}
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

	if !blockIDSlicesEqual(ndx.activeBlockIDs(), blockIDs) {
		t.Errorf("unexpected active blocks: %v wanted %v", ndx.activeBlockIDs(), blockIDs)
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
	var found bool
	for _, b := range ndx.deletedBlockIDs() {
		if b == blockID {
			found = true
		}
	}
	if !found {
		t.Errorf("expected block %q to be amongst deleted, was not found: %v", blockID, ndx.deletedBlockIDs())
	}
}

func verifyIndexBlockNotDeleted(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()

	var found bool
	for _, b := range ndx.deletedBlockIDs() {
		if b == blockID {
			found = true
		}
	}
	if found {
		t.Errorf("expected block %q to not be amongst deleted, was not found: %v", blockID, ndx.deletedBlockIDs())
	}
}

func verifyIndexBlockNotFound(t *testing.T, ndx packIndex, blockID ContentID) {
	t.Helper()
	off, size, payload, ok := ndx.getBlock(blockID)
	if ok {
		t.Errorf("block %q unexpectedly found", blockID)
	}
	if payload != nil {
		t.Errorf("block %q unexpectedly has payload", blockID)
	}
	if off != 0 {
		t.Errorf("block %q unexpectedly has an offset", blockID)
	}
	if size != 0 {
		t.Errorf("block %q unexpectedly has size", blockID)
	}
}

func verifyIndexBlockFoundPacked(t *testing.T, ndx packIndex, blockID ContentID, wantOffset, wantSize uint32) {
	t.Helper()
	off, size, payload, ok := ndx.getBlock(blockID)
	if !ok {
		t.Errorf("block %q unexpectedly not found", blockID)
		return
	}
	if payload != nil {
		t.Errorf("block %q unexpectedly has payload", blockID)
		return
	}
	if off != wantOffset {
		t.Errorf("block %q unexpectedly has an offset %v, wanted %v", blockID, off, wantOffset)
	}
	if size != wantSize {
		t.Errorf("block %q unexpectedly has size %v, wanted %v", blockID, size, wantSize)
	}
}

func verifyIndexBlockInline(t *testing.T, ndx packIndex, blockID ContentID, wantPayload []byte) {
	t.Helper()
	off, size, payload, ok := ndx.getBlock(blockID)
	if !ok {
		t.Errorf("block %q unexpectedly not found", blockID)
		return
	}
	if !reflect.DeepEqual(payload, wantPayload) {
		t.Errorf("block %q unexpectedly has payload %x, wanted %x", blockID, payload, wantPayload)
		return
	}
	if off != 0 {
		t.Errorf("block %q unexpectedly has an offset %v, wanted %v", blockID, off, 0)
	}
	if size != uint32(len(wantPayload)) {
		t.Errorf("block %q unexpectedly has a size %v, wanted %v", blockID, size, len(wantPayload))
	}
}
