package block

import (
	"fmt"
	"math"
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
	parallelFetches                = 5                // number of parallel reads goroutines
	flushPackIndexTimeout          = 10 * time.Minute // time after which all pending indexes are flushes
	packBlockPrefix                = "P"              // prefix for all storage blocks that are pack indexes
	nonPackedObjectsPackGroup      = "raw"            // ID of pack group that stores non-packed objects that don't belong to any group
	packObjectsPackGroup           = "packs"          // ID of pack group that stores pack blocks themselves
	compactedBlockSuffix           = "-z"
	maxIndexBlockUploadTime        = 1 * time.Minute
	maxNonPackedBlocksPerPackIndex = 200
)

var zeroTime time.Time

type packInfo struct {
	currentPackData  []byte
	currentPackIndex *blockmgrpb.Index
}

type blockLocation struct {
	packIndex   int
	objectIndex int
}

// Info is an information about a single block managed by Manager.
type Info struct {
	BlockID     string    `json:"blockID"`
	Length      int64     `json:"length"`
	Timestamp   time.Time `json:"time"`
	PackGroup   string    `json:"packGroup,omitempty"`
	PackBlockID string    `json:"packBlockID,omitempty"`
	PackOffset  int64     `json:"packOffset,omitempty"`
}

// Manager manages storage blocks at a low level with encryption, deduplication and packaging.
type Manager struct {
	Format FormattingOptions

	stats Stats

	cache blockCache

	mu                  sync.Mutex
	locked              bool
	groupToBlockToIndex map[string]map[string]*blockmgrpb.Index

	pendingPackIndexes    []*blockmgrpb.Index
	flushPackIndexesAfter time.Time

	openPackGroups         map[string]*packInfo
	maxPackedContentLength int
	maxPackSize            int
	formatter              Formatter

	timeNow func() time.Time
}

// DeleteBlock marks the given blockID as deleted.
//
// NOTE: To avoid race conditions only blocks that cannot be possibly re-created
// should ever be deleted. That means that contents of such blocks should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *Manager) DeleteBlock(blockID string) error {
	bm.lock()
	defer bm.unlock()

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return err
	}

	// delete from all indexes
	for _, m := range bm.groupToBlockToIndex {
		delete(m, blockID)
	}

	for _, m := range bm.openPackGroups {
		if ndx := m.currentPackIndex; ndx != nil {
			delete(ndx.Items, blockID)
		}
	}

	g := bm.ensurePackGroupLocked("", true)
	g.currentPackIndex.DeletedItems = append(g.currentPackIndex.DeletedItems, blockID)
	return nil
}

func (bm *Manager) registerUnpackedBlock(packGroupID string, blockID string, dataLength int64) error {
	bm.lock()
	defer bm.unlock()

	g := bm.registerUnpackedBlockLockedNoFlush(packGroupID, blockID, dataLength)

	if bm.timeNow().After(bm.flushPackIndexesAfter) || len(g.currentPackIndex.Items) > maxNonPackedBlocksPerPackIndex {
		if err := bm.finishPackAndMaybeFlushIndexes(g); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) registerUnpackedBlockLockedNoFlush(groupID string, blockID string, dataLength int64) *packInfo {
	bm.assertLocked()

	g := bm.ensurePackGroupLocked(groupID, true)

	// See if we already have this block ID in an unpacked pack group.
	ndx := bm.groupToBlockToIndex[groupID][blockID]
	if ndx != nil {
		return g
	}

	bm.addToIndexLocked(groupID, blockID, g.currentPackIndex, packOffsetAndSize(0, uint32(dataLength)))
	return g
}

func packOffsetAndSize(offset uint32, size uint32) uint64 {
	return uint64(offset)<<32 | uint64(size)
}

func unpackOffsetAndSize(os uint64) (uint32, uint32) {
	offset := uint32(os >> 32)
	size := uint32(os)

	return offset, size
}

func (bm *Manager) addToIndexLocked(groupID, blockID string, ndx *blockmgrpb.Index, packedOffsetAndSize uint64) {
	bm.assertLocked()

	m := bm.groupToBlockToIndex[groupID]
	if m == nil {
		m = make(map[string]*blockmgrpb.Index)
		bm.groupToBlockToIndex[groupID] = m
	}

	ndx.Items[blockID] = packedOffsetAndSize
	m[blockID] = ndx
}

func (bm *Manager) addToPackLocked(packGroup string, blockID string, data []byte, force bool) error {
	bm.assertLocked()

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return err
	}

	if !force {
		// See if we already have this block ID in the pack.
		if _, ok := bm.groupToBlockToIndex[packGroup][blockID]; ok {
			return nil
		}
	}

	g := bm.ensurePackGroupLocked(packGroup, false)

	offset := len(g.currentPackData)
	shouldFinish := offset+len(data) >= bm.maxPackSize

	g.currentPackData = append(g.currentPackData, data...)
	bm.addToIndexLocked(packGroup, blockID, g.currentPackIndex, packOffsetAndSize(uint32(offset), uint32(len(data))))

	if shouldFinish {
		if err := bm.finishPackAndMaybeFlushIndexes(g); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) finishPackAndMaybeFlushIndexes(g *packInfo) error {
	if err := bm.finishPackLocked(g); err != nil {
		return err
	}

	if bm.timeNow().After(bm.flushPackIndexesAfter) {
		if err := bm.flushPackIndexesLocked(); err != nil {
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

// Close closes block manager.
func (bm *Manager) close() error {
	return bm.cache.close()
}

func (bm *Manager) ensurePackGroupLocked(packGroup string, unpacked bool) *packInfo {
	var suffix string

	if unpacked {
		suffix = ":unpacked"
	}
	g := bm.openPackGroups[packGroup+suffix]
	if g == nil {
		g = &packInfo{}
		bm.openPackGroups[packGroup+suffix] = g
	}

	if g.currentPackIndex == nil {
		g.currentPackIndex = &blockmgrpb.Index{
			Items:           make(map[string]uint64),
			PackGroup:       packGroup,
			CreateTimeNanos: bm.timeNow().UnixNano(),
		}
		if unpacked {
			g.currentPackData = nil
		} else {
			g.currentPackData = []byte{}
		}
	}

	return g
}

func (bm *Manager) flushPackIndexesLocked() error {
	if len(bm.pendingPackIndexes) > 0 {
		if false {
			log.Printf("saving %v pack indexes", len(bm.pendingPackIndexes))
		}
		if _, err := bm.writePackIndexes(bm.pendingPackIndexes, nil); err != nil {
			return err
		}
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)
	bm.pendingPackIndexes = nil
	return nil
}

func (bm *Manager) writePackIndexes(ndx []*blockmgrpb.Index, replacesBlockBeforeTime *time.Time) (string, error) {
	data, err := proto.Marshal(&blockmgrpb.Indexes{
		Indexes: ndx,
	})
	if err != nil {
		return "", fmt.Errorf("can't encode pack index: %v", err)
	}

	var suffix string
	if replacesBlockBeforeTime != nil {
		suffix = fmt.Sprintf("%v%x", compactedBlockSuffix, replacesBlockBeforeTime.UnixNano())
	}

	inverseTimePrefix := fmt.Sprintf("%016x", math.MaxInt64-time.Now().UnixNano())

	return bm.writeUnpackedBlockNotLocked(data, packBlockPrefix+inverseTimePrefix, suffix, true)
}

func (bm *Manager) finishAllOpenPacksLocked() error {
	// finish non-pack groups first.
	for _, g := range bm.openPackGroups {
		if g.currentPackIndex != nil && g.currentPackIndex.PackGroup != packObjectsPackGroup {
			if err := bm.finishPackLocked(g); err != nil {
				return err
			}
		}
	}
	// finish pack groups at the very end.
	for _, g := range bm.openPackGroups {
		if g.currentPackIndex != nil && g.currentPackIndex.PackGroup == packObjectsPackGroup {
			if err := bm.finishPackLocked(g); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bm *Manager) finishPackLocked(g *packInfo) error {
	if g.currentPackIndex == nil {
		return nil
	}

	if len(g.currentPackIndex.Items)+len(g.currentPackIndex.DeletedItems) > 0 {
		if g.currentPackData != nil {
			dataLength := len(g.currentPackData)
			blockID, err := bm.writeUnpackedBlockNotLocked(g.currentPackData, "", "", true)
			if err != nil {
				return fmt.Errorf("can't save pack data block %q: %v", blockID, err)
			}

			bm.registerUnpackedBlockLockedNoFlush(packObjectsPackGroup, blockID, int64(dataLength))
			g.currentPackIndex.PackBlockId = blockID
		}

		bm.pendingPackIndexes = append(bm.pendingPackIndexes, g.currentPackIndex)
	}
	g.currentPackData = g.currentPackData[:0]
	g.currentPackIndex = nil

	return nil
}

// ListIndexBlocks returns the list of all index blocks, including inactive, sorted by time.
func (bm *Manager) ListIndexBlocks() ([]Info, error) {
	blocks, err := bm.cache.listIndexBlocks(true)
	if err != nil {
		return nil, fmt.Errorf("error listing index blocks: %v", err)
	}

	sortBlocksByTime(blocks)
	return blocks, nil
}

// ActiveIndexBlocks returns the list of active index blocks, sorted by time.
func (bm *Manager) ActiveIndexBlocks() ([]Info, error) {
	blocks, err := bm.cache.listIndexBlocks(false)
	if len(blocks) == 0 {
		return nil, nil
	}

	cutoffTime, err := findLatestCompactedTimestamp(blocks)
	if err != nil {
		return nil, err
	}

	var activeBlocks []Info
	for _, b := range blocks {
		if b.Timestamp.After(cutoffTime) {
			activeBlocks = append(activeBlocks, b)
		}
	}

	sortBlocksByTime(activeBlocks)
	return activeBlocks, nil
}

func sortBlocksByTime(b []Info) {
	sort.Slice(b, func(i, j int) bool {
		return b[i].Timestamp.Before(b[j].Timestamp)
	})
}

func findLatestCompactedTimestamp(blocks []Info) (time.Time, error) {
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

func (bm *Manager) loadMergedPackIndexLocked() ([]*blockmgrpb.Index, []string, time.Time, error) {
	log.Debug().Msg("listing active index blocks")
	blocks, err := bm.ActiveIndexBlocks()
	if err != nil {
		return nil, nil, zeroTime, err
	}

	if len(blocks) == 0 {
		return nil, nil, zeroTime, nil
	}

	// add block IDs to the channel
	ch := make(chan string, len(blocks))
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
	var blockIDs []string
	var indexes [][]*blockmgrpb.Index

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for b := range ch {
				data, err := bm.getBlockInternalLocked(b)
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

	var merged []*blockmgrpb.Index
	for _, pi := range indexes {
		merged = append(merged, pi...)
	}

	return merged, blockIDs, blocks[len(blocks)-1].Timestamp, nil
}

func (bm *Manager) ensurePackIndexesLoaded() error {
	bm.assertLocked()

	pi := bm.groupToBlockToIndex
	if pi != nil {
		return nil
	}

	merged, _, _, err := bm.loadMergedPackIndexLocked()
	if err != nil {
		return err
	}

	bm.groupToBlockToIndex = dedupeBlockIDsAndIndex(merged)

	totalBlocks := 0
	for _, v := range bm.groupToBlockToIndex {
		totalBlocks += len(v)
	}

	log.Debug().Int("groups", len(bm.groupToBlockToIndex)).Int("blocks", totalBlocks).Msgf("loaded indexes")

	return nil
}

func dedupeBlockIDsAndIndex(ndx []*blockmgrpb.Index) map[string]map[string]*blockmgrpb.Index {
	sort.Slice(ndx, func(i, j int) bool {
		return ndx[i].CreateTimeNanos < ndx[j].CreateTimeNanos
	})
	pi := make(map[string]map[string]*blockmgrpb.Index)
	for _, pck := range ndx {
		g := pi[pck.PackGroup]
		if g == nil {
			g = make(map[string]*blockmgrpb.Index)
			pi[pck.PackGroup] = g
		}
		for blockID := range pck.Items {
			if o := g[blockID]; o != nil {
				// this pack is same or newer.
				delete(o.Items, blockID)
			}

			g[blockID] = pck
		}

		for _, deletedBlockID := range pck.DeletedItems {
			for _, m := range pi {
				delete(m, deletedBlockID)
			}
		}
	}

	return pi
}

func removeEmptyIndexes(ndx []*blockmgrpb.Index) []*blockmgrpb.Index {
	var res []*blockmgrpb.Index
	for _, n := range ndx {
		if len(n.Items) > 0 {
			res = append(res, n)
		}
	}

	return res
}

func (bm *Manager) regroupPacksAndUnpacked(ndx []*blockmgrpb.Index) []*blockmgrpb.Index {
	var res []*blockmgrpb.Index

	allPacks := &blockmgrpb.Index{
		Items:           map[string]uint64{},
		PackGroup:       packObjectsPackGroup,
		CreateTimeNanos: bm.timeNow().UnixNano(),
	}

	allNonPacked := &blockmgrpb.Index{
		Items:           map[string]uint64{},
		PackGroup:       nonPackedObjectsPackGroup,
		CreateTimeNanos: bm.timeNow().UnixNano(),
	}

	inUsePackBlocks := map[string]bool{}

	// Iterate through all indexes, build merged index of all packs and all non-packed items.
	for _, n := range ndx {
		if n.PackGroup == packObjectsPackGroup {
			for i, o := range n.Items {
				allPacks.Items[i] = o
			}
			continue
		}

		if n.PackGroup == nonPackedObjectsPackGroup {
			for i, o := range n.Items {
				allNonPacked.Items[i] = o
			}
			continue
		}

		if n.PackBlockId != "" {
			inUsePackBlocks[n.PackBlockId] = true
		}

		res = append(res, n)
	}

	// Now delete all pack blocks that are not in use.
	for k := range allPacks.Items {
		if !inUsePackBlocks[k] {
			delete(allPacks.Items, k)
		}
	}

	if len(allPacks.Items) > 0 {
		res = append(res, allPacks)
	}

	if len(allNonPacked.Items) > 0 {
		res = append(res, allNonPacked)
	}

	return res
}

// CompactIndexes performs compaction of index blocks.
func (bm *Manager) CompactIndexes() error {
	bm.lock()
	defer bm.unlock()

	merged, blockIDs, latestBlockTime, err := bm.loadMergedPackIndexLocked()
	if err != nil {
		return err
	}

	if err := bm.compactIndexes(merged, blockIDs, latestBlockTime); err != nil {
		return err
	}

	return nil
}

// ListBlocks returns the metadata about blocks with a given prefix and kind.
func (bm *Manager) ListBlocks(prefix string, kind string) []Info {
	bm.lock()
	defer bm.unlock()

	var result []Info

	bm.ensurePackIndexesLoaded()

	packBlockIDs := map[string]bool{}
	for _, blockToIndex := range bm.groupToBlockToIndex {
		for _, b := range blockToIndex {
			packBlockIDs[b.PackBlockId] = true
		}
	}

	var blockMatches func(Info, *blockmgrpb.Index) bool

	switch kind {
	case "all":
		blockMatches = func(Info, *blockmgrpb.Index) bool { return true }

	case "logical": // blocks that are not pack blocks
		blockMatches = func(b Info, _ *blockmgrpb.Index) bool {
			return !packBlockIDs[b.BlockID]
		}

	case "packs": // blocks that are pack blocks
		blockMatches = func(b Info, _ *blockmgrpb.Index) bool {
			return packBlockIDs[b.BlockID]
		}

	case "packed": // blocks that are packed
		blockMatches = func(b Info, ndx *blockmgrpb.Index) bool {
			return ndx.PackBlockId != ""
		}

	case "nonpacked": // blocks that are not packed
		blockMatches = func(b Info, ndx *blockmgrpb.Index) bool {
			return ndx.PackBlockId == ""
		}

	default:
		blockMatches = func(Info, *blockmgrpb.Index) bool { return false }
	}

	for _, blockToIndex := range bm.groupToBlockToIndex {
		for b, ndx := range blockToIndex {
			if !strings.HasPrefix(b, prefix) {
				continue
			}

			nfo := newInfo(b, ndx)

			if !blockMatches(nfo, ndx) {
				continue
			}

			result = append(result, nfo)
		}
	}

	return result
}

func newInfo(blockID string, ndx *blockmgrpb.Index) Info {
	offset, size := unpackOffsetAndSize(ndx.Items[blockID])
	return Info{
		BlockID:     blockID,
		Length:      int64(size),
		Timestamp:   time.Unix(0, ndx.CreateTimeNanos),
		PackGroup:   ndx.PackGroup,
		PackBlockID: ndx.PackBlockId,
		PackOffset:  int64(offset),
	}
}

// ListGroupBlocks returns the list of blocks in the specified group (in random order).
func (bm *Manager) ListGroupBlocks(groupID string) []Info {
	bm.lock()
	defer bm.unlock()

	var result []Info

	bm.ensurePackIndexesLoaded()

	for blockID, ndx := range bm.groupToBlockToIndex[groupID] {
		result = append(result, newInfo(blockID, ndx))
	}

	return result
}

func (bm *Manager) compactIndexes(merged []*blockmgrpb.Index, blockIDs []string, latestBlockTime time.Time) error {
	dedupeBlockIDsAndIndex(merged)
	merged = removeEmptyIndexes(merged)
	merged = bm.regroupPacksAndUnpacked(merged)
	if len(blockIDs) <= 1 {
		log.Printf("skipping index compaction - already compacted")
		return nil
	}

	_, err := bm.writePackIndexes(merged, &latestBlockTime)
	if err != nil {
		return err
	}

	return nil
}

func makeStringChannel(s []string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		for _, v := range s {
			ch <- v
		}
	}()
	return ch
}

// Flush completes writing any pending packs and writes pack indexes to the underlyign storage.
func (bm *Manager) Flush() error {
	bm.lock()
	defer bm.unlock()

	if err := bm.finishAllOpenPacksLocked(); err != nil {
		return err
	}

	if err := bm.flushPackIndexesLocked(); err != nil {
		return err
	}

	return nil
}

// WriteBlock saves a given block of data to a pack group with a provided name and returns a blockID
// that's based on the contents of data written.
func (bm *Manager) WriteBlock(groupID string, data []byte) (string, error) {
	if bm.maxPackedContentLength > 0 && len(data) <= bm.maxPackedContentLength {
		blockID := bm.hashData(data)

		bm.lock()
		defer bm.unlock()

		err := bm.addToPackLocked(groupID, blockID, data, false)
		return blockID, err
	}

	blockID, err := bm.writeUnpackedBlockNotLocked(data, "", "", false)
	if err != nil {
		return "", err
	}

	bm.registerUnpackedBlock(nonPackedObjectsPackGroup, blockID, int64(len(data)))
	if groupID != "" {
		bm.registerUnpackedBlock(groupID, blockID, int64(len(data)))
	}
	return blockID, nil
}

// Repackage reorganizes all pack blocks belonging to a given group that are not bigger than given size.
func (bm *Manager) Repackage(groupID string, maxLength int64) error {
	bm.lock()
	defer bm.unlock()

	if groupID == "" || groupID == nonPackedObjectsPackGroup || groupID == packObjectsPackGroup {
		return fmt.Errorf("invalid group ID: %q", groupID)
	}

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return err
	}

	merged, _, _, err := bm.loadMergedPackIndexLocked()
	if err != nil {
		return err
	}

	var toRepackage []*blockmgrpb.Index
	var totalBytes int64

	for _, m := range merged {
		if m.PackGroup == groupID && m.PackBlockId != "" {
			bi, err := bm.blockInfoLocked(m.PackBlockId)
			if err != nil {
				return fmt.Errorf("unable to get info on block %q: %v", m.PackBlockId, err)
			}

			if bi.Length <= maxLength {
				toRepackage = append(toRepackage, m)
				totalBytes += bi.Length
			}
		}
	}

	log.Printf("%v blocks to re-package (%v total bytes)", len(toRepackage), totalBytes)

	for _, m := range toRepackage {
		data, err := bm.getBlockInternalLocked(m.PackBlockId)
		if err != nil {
			return fmt.Errorf("can't fetch block %q for repackaging: %v", m.PackBlockId, err)
		}

		for blockID, os := range m.Items {
			log.Printf("re-packaging: %v %v", blockID, os)

			offset, size := unpackOffsetAndSize(os)
			blockData := data[offset : offset+size]
			if err := bm.addToPackLocked(groupID, blockID, blockData, true); err != nil {
				return fmt.Errorf("unable to re-package %q: %v", blockID, err)
			}
		}
	}

	return nil
}

func (bm *Manager) writeUnpackedBlockNotLocked(data []byte, prefix string, suffix string, force bool) (string, error) {
	blockID := prefix + bm.hashData(data) + suffix

	if !force {
		// Before performing encryption, check if the block is already there.
		i, err := bm.BlockInfo(blockID)
		atomic.AddInt32(&bm.stats.CheckedBlocks, 1)
		if err == nil && i.Length == int64(len(data)) {
			atomic.AddInt32(&bm.stats.PresentBlocks, 1)
			// Block already exists in storage, correct size, return without uploading.
			return blockID, nil
		}

		if err != nil && err != storage.ErrBlockNotFound {
			// Don't know whether block exists in storage.
			return "", err
		}
	}

	// Encrypt the block in-place.
	atomic.AddInt64(&bm.stats.EncryptedBytes, int64(len(data)))
	data2, err := bm.formatter.Encrypt(data, blockID, 0)
	if err != nil {
		return "", err
	}

	atomic.AddInt32(&bm.stats.WrittenBlocks, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data)))
	if err := bm.cache.putBlock(blockID, data2); err != nil {
		return "", err
	}

	return blockID, nil
}

func (bm *Manager) hashData(data []byte) string {
	// Hash the block and compute encryption key.
	blockID := bm.formatter.ComputeBlockID(data)
	atomic.AddInt32(&bm.stats.HashedBlocks, 1)
	atomic.AddInt64(&bm.stats.HashedBytes, int64(len(data)))
	return blockID
}

func (bm *Manager) getPendingBlockLocked(blockID string) ([]byte, error) {
	bm.assertLocked()

	for _, p := range bm.openPackGroups {
		if ndx := p.currentPackIndex; ndx != nil {
			if p.currentPackData == nil {
				continue
			}

			if blk, ok := ndx.Items[blockID]; ok {
				offset, size := unpackOffsetAndSize(blk)
				return p.currentPackData[offset : offset+size], nil
			}
		}
	}
	return nil, storage.ErrBlockNotFound
}

// GetBlock gets the contents of a given block. If the block is not found returns blob.ErrBlockNotFound.
func (bm *Manager) GetBlock(blockID string) ([]byte, error) {
	bm.lock()
	defer bm.unlock()

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return nil, fmt.Errorf("can't load pack index: %v", err)
	}

	if b, err := bm.getPendingBlockLocked(blockID); err == nil {
		return b, nil
	}

	return bm.getBlockInternalLocked(blockID)
}

// BlockInfo returns information about a single block.
func (bm *Manager) BlockInfo(blockID string) (Info, error) {
	bm.lock()
	defer bm.unlock()

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return Info{}, fmt.Errorf("can't load pack index: %v", err)
	}

	return bm.blockInfoLocked(blockID)
}

func (bm *Manager) findIndexForBlockLocked(blockID string) *blockmgrpb.Index {
	bm.assertLocked()

	if ndx := bm.groupToBlockToIndex[""][blockID]; ndx != nil {
		return ndx
	}

	for _, v := range bm.groupToBlockToIndex {
		if ndx := v[blockID]; ndx != nil {
			return ndx
		}
	}

	return nil
}

func (bm *Manager) blockInfoLocked(blockID string) (Info, error) {
	if strings.HasPrefix(blockID, packBlockPrefix) {
		return Info{}, nil
	}

	bm.assertLocked()

	ndx := bm.findIndexForBlockLocked(blockID)
	if ndx == nil {
		return Info{}, storage.ErrBlockNotFound
	}

	offset, size := unpackOffsetAndSize(ndx.Items[blockID])

	return Info{
		BlockID:     blockID,
		PackGroup:   ndx.PackGroup,
		Timestamp:   time.Unix(0, ndx.CreateTimeNanos),
		PackBlockID: ndx.PackBlockId,
		PackOffset:  int64(offset),
		Length:      int64(size),
	}, nil
}

func (bm *Manager) getBlockInternalLocked(blockID string) ([]byte, error) {
	bm.assertLocked()

	s, err := bm.blockInfoLocked(blockID)
	if err != nil {
		return nil, err
	}

	var payload []byte
	underlyingBlockID := blockID
	var decryptSkip int

	if s.PackBlockID != "" {
		underlyingBlockID = s.PackBlockID
		payload, err = bm.cache.getBlock(underlyingBlockID, s.PackOffset, s.Length)
		decryptSkip = int(s.PackOffset)
	} else {
		payload, err = bm.cache.getBlock(blockID, 0, -1)
	}

	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadBlocks, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	payload, err = bm.formatter.Decrypt(payload, underlyingBlockID, decryptSkip)
	atomic.AddInt64(&bm.stats.DecryptedBytes, int64(len(payload)))
	if err != nil {
		return nil, err
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := bm.verifyChecksum(payload, blockID); err != nil {
		return nil, err
	}

	return payload, nil
}

func (bm *Manager) verifyChecksum(data []byte, blockID string) error {
	expected := bm.formatter.ComputeBlockID(data)
	if p := strings.Index(blockID, compactedBlockSuffix); p >= 0 {
		blockID = blockID[0:p]
	}
	if !strings.HasSuffix(blockID, expected) {
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
	bm.locked = false
	bm.mu.Unlock()
}

func (bm *Manager) assertLocked() {
	if !bm.locked {
		panic("must be locked")
	}
}

// listIndexBlocksFromStorage returns the list of index blocks in the given storage.
// If 'full' is set to true, this function lists and returns all blocks,
// if 'full' is false, the function returns only blocks from the last 2 compactions.
// The list of blocks is not guaranteed to be sorted.
func listIndexBlocksFromStorage(st storage.Storage, full bool) ([]Info, error) {
	maxCompactions := 2
	if full {
		maxCompactions = math.MaxInt32
	}

	ch, cancel := st.ListBlocks(packBlockPrefix)
	defer cancel()

	var results []Info
	numCompactions := 0

	var timestampCutoff time.Time
	for it := range ch {
		if !timestampCutoff.IsZero() && it.TimeStamp.Before(timestampCutoff) {
			break
		}

		if it.Error != nil {
			return nil, it.Error
		}

		results = append(results, Info{
			BlockID:   it.BlockID,
			Timestamp: it.TimeStamp,
			Length:    it.Length,
		})

		if ts, ok := getCompactedTimestamp(it.BlockID); ok {
			numCompactions++
			if numCompactions == maxCompactions {
				timestampCutoff = ts.Add(-10 * time.Minute)
			}
		}
	}

	return results, nil
}

// NewManager creates new block manager with given packing options and a formatter.
func NewManager(st storage.Storage, f FormattingOptions, caching CachingOptions) (*Manager, error) {
	sf := FormatterFactories[f.BlockFormat]
	if sf == nil {
		return nil, fmt.Errorf("unsupported block format: %v", f.BlockFormat)
	}

	formatter, err := sf(f)
	if err != nil {
		return nil, err
	}

	return &Manager{
		Format: f,

		openPackGroups:         make(map[string]*packInfo),
		timeNow:                time.Now,
		flushPackIndexesAfter:  time.Now().Add(flushPackIndexTimeout),
		pendingPackIndexes:     nil,
		maxPackedContentLength: f.MaxPackedContentLength,
		maxPackSize:            f.MaxPackSize,
		formatter:              formatter,
		cache:                  newBlockCache(st, caching),
	}, nil
}

func getCompactedTimestamp(blk string) (time.Time, bool) {
	if p := strings.Index(blk, compactedBlockSuffix); p >= 0 {
		unixNano, err := strconv.ParseInt(blk[p+len(compactedBlockSuffix):], 16, 64)
		if err != nil {
			return time.Time{}, false
		}

		return time.Unix(0, unixNano), true
	}

	return time.Time{}, false
}
