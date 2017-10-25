package block

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kopia/kopia/storage"
)

const (
	parallelFetches                = 5                // number of parallel reads goroutines
	parallelDeletes                = 20               // number of parallel delete goroutines
	flushPackIndexTimeout          = 10 * time.Minute // time after which all pending indexes are flushes
	packBlockPrefix                = "P"              // prefix for all storage blocks that are pack indexes
	nonPackedObjectsPackGroup      = "raw"            // ID of pack group that stores non-packed objects that don't belong to any group
	packObjectsPackGroup           = "packs"          // ID of pack group that stores pack blocks themselves
	maxNonPackedBlocksPerPackIndex = 200
)

type packInfo struct {
	currentPackData  []byte
	currentPackIndex *packIndex
}

type blockLocation struct {
	packIndex   int
	objectIndex int
}

// Info is an information about a single block managed by Manager.
type Info struct {
	BlockID     string
	Length      int64
	Timestamp   time.Time
	PackGroup   string
	PackBlockID string
	PackOffset  int64
}

// Manager manages storage blocks at a low level with encryption, deduplication and packaging.
type Manager struct {
	storage storage.Storage
	stats   Stats

	mu                  sync.Mutex
	locked              bool
	groupToBlockToIndex map[string]map[string]*packIndex

	pendingPackIndexes    packIndexes
	flushPackIndexesAfter time.Time

	openPackGroups         map[string]*packInfo
	maxPackedContentLength int
	maxPackSize            int
	formatter              Formatter

	timeNow func() time.Time
}

// BlockSize returns the cached size of a given block.
func (bm *Manager) BlockSize(blockID string) (int64, error) {
	bm.lock()
	defer bm.unlock()

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return 0, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := bm.findIndexForBlockLocked(blockID)
	if ndx == nil {
		return 0, storage.ErrBlockNotFound
	}

	return int64(ndx.Items[blockID].size), nil
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

	bm.addToIndexLocked(groupID, blockID, g.currentPackIndex, offsetAndSize{0, int32(dataLength)})
	return g
}

func (bm *Manager) addToIndexLocked(groupID, blockID string, ndx *packIndex, os offsetAndSize) {
	bm.assertLocked()

	m := bm.groupToBlockToIndex[groupID]
	if m == nil {
		m = make(map[string]*packIndex)
		bm.groupToBlockToIndex[groupID] = m
	}

	ndx.Items[blockID] = os
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
	bm.addToIndexLocked(packGroup, blockID, g.currentPackIndex, offsetAndSize{int32(offset), int32(len(data))})

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
		g.currentPackIndex = &packIndex{
			Items:      make(map[string]offsetAndSize),
			PackGroup:  packGroup,
			CreateTime: bm.timeNow().UTC(),
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
		log.Printf("saving %v pack indexes", len(bm.pendingPackIndexes))
		if _, err := bm.writePackIndexes(bm.pendingPackIndexes); err != nil {
			return err
		}
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)
	bm.pendingPackIndexes = nil
	return nil
}

func (bm *Manager) writePackIndexes(ndx packIndexes) (string, error) {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(ndx); err != nil {
		return "", fmt.Errorf("can't encode pack index: %v", err)
	}
	zw.Close()

	return bm.writeUnpackedBlockNotLocked(buf.Bytes(), packBlockPrefix, true)
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

	if g.currentPackData != nil {
		dataLength := len(g.currentPackData)
		blockID, err := bm.writeUnpackedBlockNotLocked(g.currentPackData, "", true)
		if err != nil {
			return fmt.Errorf("can't save pack data block %q: %v", blockID, err)
		}

		bm.registerUnpackedBlockLockedNoFlush(packObjectsPackGroup, blockID, int64(dataLength))
		g.currentPackIndex.PackBlockID = blockID
	}

	if len(g.currentPackIndex.Items) > 0 {
		bm.pendingPackIndexes = append(bm.pendingPackIndexes, g.currentPackIndex)
	}
	g.currentPackData = g.currentPackData[:0]
	g.currentPackIndex = nil

	return nil
}

func (bm *Manager) loadMergedPackIndexLocked(cutoffTime time.Time) (packIndexes, []string, error) {
	ch, cancel := bm.storage.ListBlocks(packBlockPrefix)
	defer cancel()

	t0 := time.Now()

	var wg sync.WaitGroup

	errors := make(chan error, parallelFetches)
	var mu sync.Mutex

	totalSize := 0
	var blockIDs []string
	var indexes []packIndexes

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for b := range ch {
				if b.Error != nil {
					errors <- b.Error
					return
				}

				data, err := bm.getBlockInternalLocked(b.BlockID)
				if err != nil {
					errors <- err
					return
				}

				var r io.Reader = bytes.NewReader(data)
				zr, err := gzip.NewReader(r)
				if err != nil {
					errors <- fmt.Errorf("unable to read pack index from %q: %v", b, err)
					return
				}

				pi, err := loadPackIndexes(zr)
				if err != nil {
					errors <- err
					return
				}

				if hasPackCreateAfter(pi, cutoffTime) {
					continue
				}

				mu.Lock()
				blockIDs = append(blockIDs, b.BlockID)
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
		return nil, nil, err
	}

	if false {
		log.Printf("loaded %v pack indexes (%v bytes) in %v", len(indexes), totalSize, time.Since(t0))
	}

	var merged packIndexes
	for _, pi := range indexes {
		merged = append(merged, pi...)
	}

	return merged, blockIDs, nil
}

func hasPackCreateAfter(pi packIndexes, t time.Time) bool {
	for _, ndx := range pi {
		if ndx.CreateTime.After(t) {
			return true
		}
	}

	return false
}

func (bm *Manager) ensurePackIndexesLoaded() error {
	bm.assertLocked()

	pi := bm.groupToBlockToIndex
	if pi != nil {
		return nil
	}

	t0 := time.Now()

	merged, _, err := bm.loadMergedPackIndexLocked(bm.timeNow().Add(24 * time.Hour))
	if err != nil {
		return err
	}

	bm.groupToBlockToIndex = dedupeBlockIDsAndIndex(merged)

	log.Printf("loaded %v indexes of %v blocks in %v", len(merged), len(bm.groupToBlockToIndex), time.Since(t0))

	return nil
}

func dedupeBlockIDsAndIndex(ndx packIndexes) map[string]map[string]*packIndex {
	pi := make(map[string]map[string]*packIndex)
	for _, pck := range ndx {
		g := pi[pck.PackGroup]
		if g == nil {
			g = make(map[string]*packIndex)
			pi[pck.PackGroup] = g
		}
		for blockID := range pck.Items {
			if o := g[blockID]; o != nil {
				if !pck.CreateTime.Before(o.CreateTime) {
					// this pack is same or newer.
					delete(o.Items, blockID)
					g[blockID] = pck
				} else {
					// this pack is older.
					delete(pck.Items, blockID)
				}
			} else {
				g[blockID] = pck
			}
		}
	}

	return pi
}

func removeEmptyIndexes(ndx packIndexes) packIndexes {
	var res packIndexes
	for _, n := range ndx {
		if len(n.Items) > 0 {
			res = append(res, n)
		}
	}

	return res
}

func (bm *Manager) regroupPacksAndUnpacked(ndx packIndexes) packIndexes {
	var res packIndexes

	allPacks := &packIndex{
		Items:      map[string]offsetAndSize{},
		PackGroup:  packObjectsPackGroup,
		CreateTime: bm.timeNow(),
	}

	allNonPacked := &packIndex{
		Items:      map[string]offsetAndSize{},
		PackGroup:  nonPackedObjectsPackGroup,
		CreateTime: bm.timeNow(),
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

		if n.PackBlockID != "" {
			inUsePackBlocks[n.PackBlockID] = true
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

// CompactIndexes performs compaction of index blocks and optionally removes index blocks not present in the provided set.
func (bm *Manager) CompactIndexes(cutoffTime time.Time, inUseBlocks map[string]bool) error {
	bm.lock()
	defer bm.unlock()

	merged, blockIDs, err := bm.loadMergedPackIndexLocked(cutoffTime)
	if err != nil {
		return err
	}

	if err := bm.compactIndexes(merged, blockIDs, inUseBlocks); err != nil {
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
			packBlockIDs[b.PackBlockID] = true
		}
	}

	var blockMatches func(Info, *packIndex) bool

	switch kind {
	case "all":
		blockMatches = func(Info, *packIndex) bool { return true }

	case "logical": // blocks that are not pack blocks
		blockMatches = func(b Info, _ *packIndex) bool {
			return !packBlockIDs[b.BlockID]
		}

	case "packs": // blocks that are pack blocks
		blockMatches = func(b Info, _ *packIndex) bool {
			return packBlockIDs[b.BlockID]
		}

	case "packed": // blocks that are packed
		blockMatches = func(b Info, ndx *packIndex) bool {
			return ndx.PackBlockID != ""
		}

	case "nonpacked": // blocks that are not packed
		blockMatches = func(b Info, ndx *packIndex) bool {
			return ndx.PackBlockID == ""
		}

	default:
		blockMatches = func(Info, *packIndex) bool { return false }
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

func newInfo(blockID string, ndx *packIndex) Info {
	return Info{
		BlockID:     blockID,
		Length:      int64(ndx.Items[blockID].size),
		Timestamp:   ndx.CreateTime,
		PackGroup:   ndx.PackGroup,
		PackBlockID: ndx.PackBlockID,
		PackOffset:  int64(ndx.Items[blockID].offset),
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

func (bm *Manager) compactIndexes(merged packIndexes, blockIDs []string, inUseBlocks map[string]bool) error {
	dedupeBlockIDsAndIndex(merged)
	if inUseBlocks != nil {
		for _, m := range merged {
			for b := range m.Items {
				if !inUseBlocks[b] {
					//log.Printf("removing block in index but not in use: %q", b)
					delete(m.Items, b)
				}
			}
		}
	}

	merged = removeEmptyIndexes(merged)
	merged = bm.regroupPacksAndUnpacked(merged)

	if len(blockIDs) <= 1 && inUseBlocks == nil {
		log.Printf("skipping index compaction - already compacted")
		return nil
	}

	compactedBlock, err := bm.writePackIndexes(merged)
	if err != nil {
		return err
	}

	ch := makeStringChannel(blockIDs)
	var wg sync.WaitGroup

	for i := 0; i < parallelDeletes; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for blockID := range ch {
				if blockID == compactedBlock {
					log.Printf("warning: sanity check failed, not deleting freshly-written compacted index: %q", compactedBlock)
					continue
				}
				if err := bm.storage.DeleteBlock(blockID); err != nil {
					log.Printf("warning: unable to delete %q: %v", blockID, err)
				}
			}
		}(i)
	}
	wg.Wait()
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

	blockID, err := bm.writeUnpackedBlockNotLocked(data, "", false)
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
func (bm *Manager) Repackage(groupID string, maxLength int64, cutoffTime time.Time) error {
	bm.lock()
	defer bm.unlock()

	if groupID == "" || groupID == nonPackedObjectsPackGroup || groupID == packObjectsPackGroup {
		return fmt.Errorf("invalid group ID: %q", groupID)
	}

	if err := bm.ensurePackIndexesLoaded(); err != nil {
		return err
	}

	merged, _, err := bm.loadMergedPackIndexLocked(cutoffTime)
	if err != nil {
		return err
	}

	var toRepackage []*packIndex
	var totalBytes int64

	for _, m := range merged {
		if m.PackGroup == groupID && m.PackBlockID != "" {
			bi, err := bm.blockInfoLocked(m.PackBlockID)
			if err != nil {
				return fmt.Errorf("unable to get info on block %q: %v", m.PackBlockID, err)
			}

			if bi.Length <= maxLength {
				toRepackage = append(toRepackage, m)
				totalBytes += bi.Length
			}
		}
	}

	log.Printf("%v blocks to re-package (%v total bytes)", len(toRepackage), totalBytes)

	for _, m := range toRepackage {
		data, err := bm.getBlockInternalLocked(m.PackBlockID)
		if err != nil {
			return fmt.Errorf("can't fetch block %q for repackaging: %v", m.PackBlockID, err)
		}

		for blockID, os := range m.Items {
			log.Printf("re-packaging: %v %v", blockID, os)

			blockData := data[os.offset : os.offset+os.size]
			if err := bm.addToPackLocked(groupID, blockID, blockData, true); err != nil {
				return fmt.Errorf("unable to re-package %q: %v", blockID, err)
			}
		}
	}

	return nil
}

func (bm *Manager) writeUnpackedBlockNotLocked(data []byte, prefix string, force bool) (string, error) {
	blockID := prefix + bm.hashData(data)

	if !force {
		// Before performing encryption, check if the block is already there.
		blockSize, err := bm.BlockSize(blockID)
		atomic.AddInt32(&bm.stats.CheckedBlocks, 1)
		if err == nil && blockSize == int64(len(data)) {
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
	if err := bm.storage.PutBlock(blockID, data2); err != nil {
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
				return p.currentPackData[blk.offset : blk.offset+blk.size], nil
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

func (bm *Manager) findIndexForBlockLocked(blockID string) *packIndex {
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
	bm.assertLocked()

	ndx := bm.findIndexForBlockLocked(blockID)
	if ndx == nil {
		return Info{}, storage.ErrBlockNotFound
	}

	return Info{
		BlockID:     blockID,
		PackGroup:   ndx.PackGroup,
		Timestamp:   ndx.CreateTime,
		PackBlockID: ndx.PackBlockID,
		PackOffset:  int64(ndx.Items[blockID].offset),
		Length:      int64(ndx.Items[blockID].size),
	}, nil
}

func (bm *Manager) getBlockInternalLocked(blockID string) ([]byte, error) {
	bm.assertLocked()

	s, err := bm.blockInfoLocked(blockID)
	if err != nil {
		if err != storage.ErrBlockNotFound {
			return nil, err
		}
	}

	var payload []byte
	underlyingBlockID := blockID
	var decryptSkip int

	if s.PackBlockID != "" {
		underlyingBlockID = s.PackBlockID
		payload, err = bm.storage.GetBlock(underlyingBlockID, s.PackOffset, s.Length)
		decryptSkip = int(s.PackOffset)
	} else {
		payload, err = bm.storage.GetBlock(blockID, 0, -1)
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

// NewManager creates new block manager with given packing options and a formatter.
func NewManager(st storage.Storage, maxPackedContentLength, maxPackSize int, formatter Formatter) *Manager {
	return &Manager{
		storage:                st,
		openPackGroups:         make(map[string]*packInfo),
		timeNow:                time.Now,
		flushPackIndexesAfter:  time.Now().Add(flushPackIndexTimeout),
		pendingPackIndexes:     nil,
		maxPackedContentLength: maxPackedContentLength,
		maxPackSize:            maxPackSize,
		formatter:              formatter,
	}
}
