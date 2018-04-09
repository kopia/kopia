package block

import (
	"bytes"
	"context"
	"crypto/aes"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/internal/blockmgrpb"
	"github.com/kopia/kopia/storage"
)

const (
	parallelFetches          = 5                // number of parallel reads goroutines
	flushPackIndexTimeout    = 10 * time.Minute // time after which all pending indexes are flushes
	indexBlockPrefix         = "i"              // prefix for all storage blocks that are pack indexes
	compactedBlockSuffix     = "-z"
	defaultMinPreambleLength = 32
	defaultMaxPreambleLength = 32
	defaultPaddingUnit       = 4096
	maxInlineContentLength   = 100000 // amount of block data to store in the index block itself

	autoCompactionBlockCount   = 16
	autoCompactionSafetyMargin = 5 * time.Minute // do not auto-compact if time since block was written is less than this
)

var zeroTime time.Time

// ContentID uniquely identifies a block of content stored in repository.
type ContentID string

// Info is an information about a single block managed by Manager.
type Info struct {
	BlockID     ContentID       `json:"blockID"`
	Length      int64           `json:"length"`
	Timestamp   time.Time       `json:"time"`
	PackBlockID PhysicalBlockID `json:"packBlockID,omitempty"`
	PackOffset  int64           `json:"packOffset,omitempty"`
}

// PhysicalBlockID identifies physical storage block.
type PhysicalBlockID string

// IndexInfo is an information about a single index block managed by Manager.
type IndexInfo struct {
	BlockID   PhysicalBlockID `json:"blockID"`
	Length    int64           `json:"length"`
	Timestamp time.Time       `json:"time"`
}

// Manager manages storage blocks at a low level with encryption, deduplication and packaging.
type Manager struct {
	Format FormattingOptions

	stats Stats
	cache blockCache

	mu                      sync.Mutex
	locked                  bool
	checkInvariantsOnUnlock bool

	blockIDToIndex     map[ContentID]packIndex       // maps block ID to corresponding index
	packBlockIDToIndex map[PhysicalBlockID]packIndex // maps pack block ID to corresponding index

	currentPackData  []byte    // data for the current block
	currentPackIndex packIndex // index of a current block

	pendingPackIndexes    []packIndex // pending indexes of blocks that have been saved.
	flushPackIndexesAfter time.Time   // time when those indexes should be flushed

	maxInlineContentLength int
	maxPackSize            int
	formatter              Formatter

	minPreambleLength int
	maxPreambleLength int
	paddingUnit       int
	timeNow           func() time.Time
}

// DeleteBlock marks the given blockID as deleted.
//
// NOTE: To avoid race conditions only blocks that cannot be possibly re-created
// should ever be deleted. That means that contents of such blocks should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *Manager) DeleteBlock(blockID ContentID) error {
	bm.lock()
	defer bm.unlock()

	delete(bm.blockIDToIndex, blockID)
	bm.currentPackIndex.deleteBlock(blockID, true)

	return nil
}

func (bm *Manager) addToIndexLocked(blockID ContentID, ndx packIndex, offset, size uint32) {
	bm.assertLocked()

	ndx.addPackedBlock(blockID, offset, size)
	bm.blockIDToIndex[blockID] = ndx
}

func (bm *Manager) addToPackLocked(ctx context.Context, blockID ContentID, data []byte) error {
	bm.assertLocked()

	if len(bm.currentPackData) == 0 && bm.maxPreambleLength > 0 {
		preambleLength := rand.Intn(bm.maxPreambleLength-bm.minPreambleLength+1) + bm.minPreambleLength
		preamble := make([]byte, preambleLength, preambleLength+len(data))
		if _, err := io.ReadFull(cryptorand.Reader, preamble); err != nil {
			return err
		}

		bm.currentPackData = preamble
	}

	offset := len(bm.currentPackData)
	shouldFinish := offset+len(data) >= bm.maxPackSize

	bm.currentPackData = append(bm.currentPackData, data...)
	bm.addToIndexLocked(blockID, bm.currentPackIndex, uint32(offset), uint32(len(data)))

	if shouldFinish {
		if err := bm.finishPackAndMaybeFlushIndexes(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) finishPackAndMaybeFlushIndexes(ctx context.Context) error {
	if err := bm.finishPackLocked(ctx); err != nil {
		return err
	}

	if bm.timeNow().After(bm.flushPackIndexesAfter) {
		if err := bm.flushPackIndexesLocked(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Stats returns statistics about block manager operations.
func (bm *Manager) Stats() Stats {
	return bm.stats
}

// ResetStats resets statistics to zero values.
func (bm *Manager) ResetStats() {
	bm.stats = Stats{}
}

func (bm *Manager) verifyInvariantsLocked() {
	bm.assertLocked()

	bm.verifyEachBlockHasTargetIndexEntryLocked()
	bm.verifyPackBlockIndexLocked()
	bm.verifyPendingPackIndexesAreRegisteredLocked()
}

func (bm *Manager) verifyPendingPackIndexesAreRegisteredLocked() {
	// each pending pack index is registered
	for _, p := range bm.pendingPackIndexes {
		for _, blkID := range p.activeBlockIDs() {
			if _, ok := bm.blockIDToIndex[blkID]; !ok {
				bm.invariantViolated("invariant violated - pending block %q not in index", blkID)
			}
		}
	}
}

func (bm *Manager) verifyPackBlockIndexLocked() {
	for packBlockID, ndx := range bm.packBlockIDToIndex {
		if ndx.packBlockID() != packBlockID {
			bm.invariantViolated("invariant violated - pack %q not matching its pack block ID", packBlockID)
		}
	}
}

func (bm *Manager) verifyEachBlockHasTargetIndexEntryLocked() {
	// verify that each block in blockIDToIndex has a corresponding entry in the target index.
	for blkID, ndx := range bm.blockIDToIndex {
		off, size, payload, ok := ndx.getBlock(blkID)
		if !ok {
			bm.invariantViolated("invariant violated - block %q not found within its pack", blkID)
			continue
		}
		if payload != nil {
			// inline
			continue
		}
		if ndx.packLength() > 0 && uint64(off)+uint64(size) > ndx.packLength() {
			bm.invariantViolated("invariant violated - block %q out of bounds within its pack (%v,%v) vs %v", blkID, off, size, ndx.packLength())
		}
		continue

	}
}

func (bm *Manager) invariantViolated(msg string, args ...interface{}) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	panic(msg)
}

func (bm *Manager) startPackIndexLocked() {
	bm.currentPackIndex = protoPackIndexV1{&blockmgrpb.IndexV1{
		Items:           make(map[string]uint64),
		InlineItems:     make(map[string][]byte),
		CreateTimeNanos: uint64(bm.timeNow().UnixNano()),
	}}
	bm.currentPackData = []byte{}
}

func (bm *Manager) flushPackIndexesLocked(ctx context.Context) error {
	if len(bm.pendingPackIndexes) > 0 {
		if false {
			log.Printf("saving %v pack indexes", len(bm.pendingPackIndexes))
		}
		if _, err := bm.writePackIndexes(ctx, bm.pendingPackIndexes, nil); err != nil {
			return err
		}
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)
	bm.pendingPackIndexes = nil
	return nil
}

func (bm *Manager) writePackIndexes(ctx context.Context, ndx []packIndex, replacesBlockBeforeTime *time.Time) (PhysicalBlockID, error) {
	pb := &blockmgrpb.Indexes{}

	for _, n := range ndx {
		n.addToIndexes(pb)
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		return "", fmt.Errorf("can't encode pack index: %v", err)
	}

	var suffix string
	if replacesBlockBeforeTime != nil {
		suffix = fmt.Sprintf("%v%x", compactedBlockSuffix, replacesBlockBeforeTime.UnixNano())
	}

	inverseTimePrefix := fmt.Sprintf("%016x", math.MaxInt64-time.Now().UnixNano())

	return bm.writeUnpackedBlockNotLocked(ctx, data, indexBlockPrefix+inverseTimePrefix, suffix)
}

func (bm *Manager) finishPackLocked(ctx context.Context) error {
	if !bm.currentPackIndex.isEmpty() {
		log.Debug().Msg("finishing pack")
		if len(bm.currentPackData) < bm.maxInlineContentLength {
			bm.currentPackIndex.packedToInline(bm.currentPackData)
			bm.currentPackData = nil
		}
		if bm.currentPackData != nil {
			if bm.paddingUnit > 0 {
				if missing := bm.paddingUnit - (len(bm.currentPackData) % bm.paddingUnit); missing > 0 {
					postamble := make([]byte, missing)
					if _, err := io.ReadFull(cryptorand.Reader, postamble); err != nil {
						return fmt.Errorf("can't allocate random bytes for postamble: %v", err)
					}
					bm.currentPackData = append(bm.currentPackData, postamble...)
				}
			}
			packBlockID, err := bm.writeUnpackedBlockNotLocked(ctx, bm.currentPackData, "", "")
			if err != nil {
				return fmt.Errorf("can't save pack data block %q: %v", packBlockID, err)
			}

			bm.currentPackIndex.finishPack(packBlockID, uint64(len(bm.currentPackData)))
			bm.packBlockIDToIndex[packBlockID] = bm.currentPackIndex
		}

		bm.pendingPackIndexes = append(bm.pendingPackIndexes, bm.currentPackIndex)
	} else {
		log.Printf("nothing to write - pack is empty")
	}

	bm.startPackIndexLocked()

	return nil
}

// ListIndexBlocks returns the list of all index blocks, including inactive, sorted by time.
func (bm *Manager) ListIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	blocks, err := bm.cache.listIndexBlocks(ctx, true)
	if err != nil {
		return nil, fmt.Errorf("error listing index blocks: %v", err)
	}

	sortBlocksByTime(blocks)
	return blocks, nil
}

// ActiveIndexBlocks returns the list of active index blocks, sorted by time.
func (bm *Manager) ActiveIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	blocks, err := bm.cache.listIndexBlocks(ctx, false)
	if err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, nil
	}

	cutoffTime, err := findLatestCompactedTimestamp(blocks)
	if err != nil {
		return nil, err
	}

	var activeBlocks []IndexInfo
	for _, b := range blocks {
		if b.Timestamp.After(cutoffTime) {
			activeBlocks = append(activeBlocks, b)
		}
	}

	sortBlocksByTime(activeBlocks)
	return activeBlocks, nil
}

func sortBlocksByTime(b []IndexInfo) {
	sort.Slice(b, func(i, j int) bool {
		return b[i].Timestamp.Before(b[j].Timestamp)
	})
}

func findLatestCompactedTimestamp(blocks []IndexInfo) (time.Time, error) {
	// look for blocks that end with -ztimestamp
	// find the latest such timestamp.
	var latestTime time.Time

	for _, b := range blocks {
		blk := b.BlockID
		if ts, ok := getCompactedTimestamp(blk); ok {
			if ts.After(latestTime) {
				latestTime = ts
			}
		}
	}

	return latestTime, nil
}

func (bm *Manager) loadMergedPackIndexLocked(ctx context.Context) ([]packIndex, []PhysicalBlockID, time.Time, error) {
	log.Debug().Msg("listing active index blocks")
	blocks, err := bm.ActiveIndexBlocks(ctx)
	if err != nil {
		return nil, nil, zeroTime, err
	}

	if len(blocks) == 0 {
		return nil, nil, zeroTime, nil
	}

	// add block IDs to the channel
	ch := make(chan PhysicalBlockID, len(blocks))
	go func() {
		for _, b := range blocks {
			ch <- b.BlockID
		}
		close(ch)
	}()

	log.Debug().Int("parallelism", parallelFetches).Int("count", len(blocks)).Msg("loading active blocks")

	var wg sync.WaitGroup

	errors := make(chan error, parallelFetches)
	var mu sync.Mutex

	totalSize := 0
	var blockIDs []PhysicalBlockID
	var indexes [][]packIndex

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for b := range ch {
				data, err := bm.getPhysicalBlockInternalLocked(ctx, b)
				if err != nil {
					errors <- err
					return
				}

				pi, err := loadPackIndexes(data)
				if err != nil {
					errors <- err
					return
				}

				mu.Lock()
				blockIDs = append(blockIDs, b)
				indexes = append(indexes, pi)
				totalSize += len(data)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Propagate async errors, if any.
	for err := range errors {
		return nil, nil, time.Now(), err
	}

	var merged []packIndex
	for _, pi := range indexes {
		merged = append(merged, pi...)
	}

	return merged, blockIDs, blocks[len(blocks)-1].Timestamp, nil
}

func (bm *Manager) initializeIndexes(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	merged, blockIDs, latestBlockTime, err := bm.loadMergedPackIndexLocked(ctx)
	if err != nil {
		return err
	}
	log.Debug().Msgf("loaded %v index blocks with latest time %v", len(blockIDs), latestBlockTime.Local())

	bm.blockIDToIndex, bm.packBlockIDToIndex = dedupeBlockIDsAndIndex(merged)
	if len(blockIDs) >= autoCompactionBlockCount && latestBlockTime.Before(time.Now().Add(-autoCompactionSafetyMargin)) {
		log.Debug().Msgf("auto compacting block indexes (block count %v exceeds threshold of %v)", len(blockIDs), autoCompactionBlockCount)
		merged = removeEmptyIndexes(merged)
		if _, err := bm.writePackIndexes(ctx, merged, &latestBlockTime); err != nil {
			return err
		}
	}

	totalBlocks := len(bm.blockIDToIndex)
	log.Debug().Int("blocks", totalBlocks).Msgf("loaded indexes")

	return nil
}

func dedupeBlockIDsAndIndex(ndx []packIndex) (blockToIndex map[ContentID]packIndex, packToIndex map[PhysicalBlockID]packIndex) {
	sort.Slice(ndx, func(i, j int) bool {
		return ndx[i].createTimeNanos() < ndx[j].createTimeNanos()
	})
	blockToIndex = make(map[ContentID]packIndex)
	packToIndex = make(map[PhysicalBlockID]packIndex)
	for _, pck := range ndx {
		packToIndex[pck.packBlockID()] = pck
		for _, blockID := range pck.activeBlockIDs() {
			if o := blockToIndex[blockID]; o != nil {
				o.deleteBlock(blockID, false)
			}

			blockToIndex[blockID] = pck
		}

		for _, deletedBlockID := range pck.deletedBlockIDs() {
			delete(blockToIndex, deletedBlockID)
		}
	}

	return
}

func removeEmptyIndexes(ndx []packIndex) []packIndex {
	var res []packIndex
	for _, n := range ndx {
		if !n.isEmpty() {
			res = append(res, n)
		}
	}

	return res
}

// CompactIndexes performs compaction of index blocks.
func (bm *Manager) CompactIndexes(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	merged, indexBlocks, latestBlockTime, err := bm.loadMergedPackIndexLocked(ctx)
	if err != nil {
		return err
	}

	if err := bm.compactIndexes(ctx, merged, indexBlocks, latestBlockTime); err != nil {
		return err
	}

	return nil
}

// ListBlocks returns the metadata about blocks with a given prefix and kind.
func (bm *Manager) ListBlocks(prefix ContentID) ([]Info, error) {
	bm.lock()
	defer bm.unlock()

	var result []Info

	for b, ndx := range bm.blockIDToIndex {
		if !strings.HasPrefix(string(b), string(prefix)) {
			continue
		}

		i, err := newInfo(b, ndx)
		if err != nil {
			return nil, fmt.Errorf("block %v not found", b)
		}
		result = append(result, i)
	}

	return result, nil
}

func newInfo(blockID ContentID, ndx packIndex) (Info, error) {
	offset, size, inline, ok := ndx.getBlock(blockID)
	if !ok {
		return Info{}, storage.ErrBlockNotFound
	}
	if inline != nil {
		return Info{
			BlockID:     blockID,
			Length:      int64(len(inline)),
			Timestamp:   time.Unix(0, int64(ndx.createTimeNanos())),
			PackBlockID: ndx.packBlockID(),
		}, nil
	}

	return Info{
		BlockID:     blockID,
		Length:      int64(size),
		Timestamp:   time.Unix(0, int64(ndx.createTimeNanos())),
		PackBlockID: ndx.packBlockID(),
		PackOffset:  int64(offset),
	}, nil
}

func (bm *Manager) compactIndexes(ctx context.Context, merged []packIndex, blockIDs []PhysicalBlockID, latestBlockTime time.Time) error {
	dedupeBlockIDsAndIndex(merged)
	merged = removeEmptyIndexes(merged)
	if len(blockIDs) <= 1 {
		log.Printf("skipping index compaction - already compacted")
		return nil
	}

	_, err := bm.writePackIndexes(ctx, merged, &latestBlockTime)
	return err
}

// Flush completes writing any pending packs and writes pack indexes to the underlyign storage.
func (bm *Manager) Flush(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	if err := bm.finishPackLocked(ctx); err != nil {
		return err
	}

	if err := bm.flushPackIndexesLocked(ctx); err != nil {
		return err
	}

	return nil
}

// WriteBlock saves a given block of data to a pack group with a provided name and returns a blockID
// that's based on the contents of data written.
func (bm *Manager) WriteBlock(ctx context.Context, data []byte, prefix ContentID) (ContentID, error) {
	blockID := prefix + ContentID(hex.EncodeToString(bm.hashData(data)))

	bm.lock()
	defer bm.unlock()

	// See if we already have this block ID in the pack.
	if _, ok := bm.blockIDToIndex[blockID]; ok {
		return blockID, nil
	}

	err := bm.addToPackLocked(ctx, blockID, data)
	return blockID, err
}

// IsStorageBlockInUse determines whether given storage block is in use by currently loaded pack indexes.
func (bm *Manager) IsStorageBlockInUse(storageBlockID PhysicalBlockID) bool {
	bm.lock()
	defer bm.unlock()

	return bm.packBlockIDToIndex[storageBlockID] != nil
}

// Repackage reorganizes all pack blocks belonging to a given group that are not bigger than given size.
func (bm *Manager) Repackage(ctx context.Context, maxLength uint64) error {
	bm.lock()
	defer bm.unlock()

	merged, _, _, err := bm.loadMergedPackIndexLocked(ctx)
	if err != nil {
		return err
	}

	var toRepackage []packIndex
	var totalBytes uint64

	for _, m := range merged {
		bi, ok := bm.packBlockIDToIndex[m.packBlockID()]
		if !ok {
			return fmt.Errorf("unable to get info on pack block %q", m.packBlockID())
		}

		if bi.packLength() <= maxLength {
			toRepackage = append(toRepackage, m)
			totalBytes += bi.packLength()
		}
	}

	done := map[ContentID]bool{}

	if len(toRepackage) <= 1 {
		log.Printf("nothing to do (%v total bytes)", totalBytes)
		return nil
	}

	log.Printf("%v blocks to re-package (%v total bytes)", len(toRepackage), totalBytes)

	for _, m := range toRepackage {
		if err := bm.repackageBlock(ctx, m, done); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) repackageBlock(ctx context.Context, m packIndex, done map[ContentID]bool) error {
	data, err := bm.getPhysicalBlockInternalLocked(ctx, m.packBlockID())
	if err != nil {
		return fmt.Errorf("can't fetch block %q for repackaging: %v", m.packBlockID(), err)
	}

	for _, blockID := range m.activeBlockIDs() {
		if done[blockID] {
			continue
		}
		done[blockID] = true
		offset, size, payload, ok := m.getBlock(blockID)
		if !ok {
			return fmt.Errorf("unable to get packed block to repackage")
		}
		if payload == nil {
			payload = data[offset : offset+size]
		}
		if err := bm.addToPackLocked(ctx, blockID, payload); err != nil {
			return fmt.Errorf("unable to re-package %q: %v", blockID, err)
		}
	}

	return nil
}

func (bm *Manager) writeUnpackedBlockNotLocked(ctx context.Context, data []byte, prefix string, suffix string) (PhysicalBlockID, error) {
	hash := bm.hashData(data)
	physicalBlockID := PhysicalBlockID(prefix + hex.EncodeToString(hash) + suffix)

	// Encrypt the block in-place.
	atomic.AddInt64(&bm.stats.EncryptedBytes, int64(len(data)))
	data2, err := bm.formatter.Encrypt(data, hash, 0)
	if err != nil {
		return "", err
	}

	atomic.AddInt32(&bm.stats.WrittenBlocks, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data)))
	if err := bm.cache.putBlock(ctx, physicalBlockID, data2); err != nil {
		return "", err
	}

	return physicalBlockID, nil
}

func (bm *Manager) hashData(data []byte) []byte {
	// Hash the block and compute encryption key.
	blockID := bm.formatter.ComputeBlockID(data)
	atomic.AddInt32(&bm.stats.HashedBlocks, 1)
	atomic.AddInt64(&bm.stats.HashedBytes, int64(len(data)))
	return blockID
}

func (bm *Manager) getPendingBlockLocked(blockID ContentID) ([]byte, error) {
	bm.assertLocked()

	if ndx := bm.currentPackIndex; ndx != nil {
		offset, size, payload, ok := bm.currentPackIndex.getBlock(blockID)
		if ok {
			if payload != nil {
				return payload, nil
			}

			if bm.currentPackData != nil {
				return bm.currentPackData[offset : offset+size], nil
			}
		}
	}

	return nil, storage.ErrBlockNotFound
}

// GetBlock gets the contents of a given block. If the block is not found returns blob.ErrBlockNotFound.
func (bm *Manager) GetBlock(ctx context.Context, blockID ContentID) ([]byte, error) {
	bm.lock()
	defer bm.unlock()

	if b, err := bm.getPendingBlockLocked(blockID); err == nil {
		return b, nil
	}

	return bm.getPackedBlockInternalLocked(ctx, blockID)
}

// GetIndexBlock gets the contents of a given index block. If the block is not found returns blob.ErrBlockNotFound.
func (bm *Manager) GetIndexBlock(ctx context.Context, blockID PhysicalBlockID) ([]byte, error) {
	bm.lock()
	defer bm.unlock()

	return bm.getPhysicalBlockInternalLocked(ctx, blockID)
}

// BlockInfo returns information about a single block.
func (bm *Manager) BlockInfo(ctx context.Context, blockID ContentID) (Info, error) {
	bm.lock()
	defer bm.unlock()

	return bm.packedBlockInfoLocked(blockID)
}

func (bm *Manager) findIndexForBlockLocked(blockID ContentID) packIndex {
	bm.assertLocked()

	return bm.blockIDToIndex[blockID]
}

func (bm *Manager) packedBlockInfoLocked(blockID ContentID) (Info, error) {
	ndx := bm.findIndexForBlockLocked(blockID)
	if ndx == nil {
		return Info{}, storage.ErrBlockNotFound
	}

	return newInfo(blockID, ndx)
}

func (bm *Manager) getPackedBlockInternalLocked(ctx context.Context, blockID ContentID) ([]byte, error) {
	bm.assertLocked()

	ndx, ok := bm.blockIDToIndex[blockID]
	if !ok {
		return nil, storage.ErrBlockNotFound
	}

	offset, size, payload, ok := ndx.getBlock(blockID)
	if !ok {
		return nil, storage.ErrBlockNotFound
	}

	if payload != nil {
		return payload, nil
	}

	underlyingBlockID := ndx.packBlockID()
	payload, err := bm.cache.getBlock(ctx, string(blockID), underlyingBlockID, int64(offset), int64(size))
	decryptSkip := int(offset)

	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadBlocks, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	iv, err := getPhysicalBlockIV(underlyingBlockID)
	if err != nil {
		return nil, err
	}

	payload, err = bm.formatter.Decrypt(payload, iv, decryptSkip)
	atomic.AddInt64(&bm.stats.DecryptedBytes, int64(len(payload)))
	if err != nil {
		return nil, err
	}

	iv2, err := getPackedBlockIV(blockID)
	if err != nil {
		return nil, err
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := bm.verifyChecksum(payload, iv2); err != nil {
		return nil, err
	}

	return payload, nil
}

func (bm *Manager) getPhysicalBlockInternalLocked(ctx context.Context, blockID PhysicalBlockID) ([]byte, error) {
	bm.assertLocked()

	payload, err := bm.cache.getBlock(ctx, string(blockID), blockID, 0, -1)
	if err != nil {
		return nil, err
	}

	iv, err := getPhysicalBlockIV(blockID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadBlocks, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	payload, err = bm.formatter.Decrypt(payload, iv, 0)
	atomic.AddInt64(&bm.stats.DecryptedBytes, int64(len(payload)))
	if err != nil {
		return nil, err
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := bm.verifyChecksum(payload, iv); err != nil {
		return nil, err
	}

	return payload, nil
}

func getPackedBlockIV(s ContentID) ([]byte, error) {
	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):]))
}

func getPhysicalBlockIV(b PhysicalBlockID) ([]byte, error) {
	s := string(b)
	if p := strings.Index(s, "-"); p >= 0 {
		s = s[0:p]
	}
	return hex.DecodeString(s[len(s)-(aes.BlockSize*2):])
}

func (bm *Manager) verifyChecksum(data []byte, blockID []byte) error {
	expected := bm.formatter.ComputeBlockID(data)
	if !bytes.HasSuffix(blockID, expected) {
		atomic.AddInt32(&bm.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v', expected %v", blockID, expected)
	}

	atomic.AddInt32(&bm.stats.ValidBlocks, 1)
	return nil
}

func (bm *Manager) lock() {
	bm.mu.Lock()
	bm.locked = true
}

func (bm *Manager) unlock() {
	if bm.checkInvariantsOnUnlock {
		bm.verifyInvariantsLocked()
	}

	bm.locked = false
	bm.mu.Unlock()
}

func (bm *Manager) assertLocked() {
	if !bm.locked {
		panic("must be locked")
	}
}

type cachedList struct {
	Timestamp time.Time   `json:"timestamp"`
	Blocks    []IndexInfo `json:"blocks"`
}

// listIndexBlocksFromStorage returns the list of index blocks in the given storage.
// If 'full' is set to true, this function lists and returns all blocks,
// if 'full' is false, the function returns only blocks from the last 2 compactions.
// The list of blocks is not guaranteed to be sorted.
func listIndexBlocksFromStorage(ctx context.Context, st storage.Storage, full bool) ([]IndexInfo, error) {
	maxCompactions := 2
	if full {
		maxCompactions = math.MaxInt32
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := st.ListBlocks(ctx, indexBlockPrefix)

	var results []IndexInfo
	numCompactions := 0

	var timestampCutoff time.Time
	for it := range ch {
		if !timestampCutoff.IsZero() && it.TimeStamp.Before(timestampCutoff) {
			break
		}

		if it.Error != nil {
			return nil, it.Error
		}

		ii := IndexInfo{
			BlockID:   PhysicalBlockID(it.BlockID),
			Timestamp: it.TimeStamp,
			Length:    it.Length,
		}
		results = append(results, ii)

		if ts, ok := getCompactedTimestamp(ii.BlockID); ok {
			numCompactions++
			if numCompactions == maxCompactions {
				timestampCutoff = ts.Add(-10 * time.Minute)
			}
		}
	}

	return results, nil
}

// NewManager creates new block manager with given packing options and a formatter.
func NewManager(ctx context.Context, st storage.Storage, f FormattingOptions, caching CachingOptions) (*Manager, error) {
	return newManagerWithTime(ctx, st, f, caching, time.Now)
}

func newManagerWithTime(ctx context.Context, st storage.Storage, f FormattingOptions, caching CachingOptions, timeNow func() time.Time) (*Manager, error) {
	sf := FormatterFactories[f.BlockFormat]
	if sf == nil {
		return nil, fmt.Errorf("unsupported block format: %v", f.BlockFormat)
	}

	formatter, err := sf(f)
	if err != nil {
		return nil, err
	}

	cache, err := newBlockCache(ctx, st, caching)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize cache: %v", err)
	}

	m := &Manager{
		Format:                 f,
		timeNow:                timeNow,
		flushPackIndexesAfter:  timeNow().Add(flushPackIndexTimeout),
		pendingPackIndexes:     nil,
		maxPackSize:            f.MaxPackSize,
		formatter:              formatter,
		blockIDToIndex:         make(map[ContentID]packIndex),
		packBlockIDToIndex:     make(map[PhysicalBlockID]packIndex),
		minPreambleLength:      defaultMinPreambleLength,
		maxPreambleLength:      defaultMaxPreambleLength,
		paddingUnit:            defaultPaddingUnit,
		maxInlineContentLength: maxInlineContentLength,
		cache: cache,
	}

	if os.Getenv("KOPIA_VERIFY_INVARIANTS") != "" {
		m.checkInvariantsOnUnlock = true
	}

	m.startPackIndexLocked()

	if err := m.initializeIndexes(ctx); err != nil {
		return nil, fmt.Errorf("unable initialize indexes: %v", err)
	}

	return m, nil
}
func getCompactedTimestamp(blk PhysicalBlockID) (time.Time, bool) {
	if p := strings.Index(string(blk), compactedBlockSuffix); p >= 0 {
		unixNano, err := strconv.ParseInt(string(blk)[p+len(compactedBlockSuffix):], 16, 64)
		if err != nil {
			return time.Time{}, false
		}

		return time.Unix(0, unixNano), true
	}

	return time.Time{}, false
}
