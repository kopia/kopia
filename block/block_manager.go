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

const parallelFetches = 5
const parallelDeletes = 20
const flushPackIndexTimeout = 10 * time.Minute
const packObjectPrefix = "P"
const legacyUnpackedObjectsPackGroup = "_unpacked_"
const nonPackedObjectsPackGroup = "raw"
const packObjectsPackGroup = "packs"
const maxNonPackedBlocksPerPackIndex = 200

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

	mu           sync.Mutex
	blockToIndex map[string]*packIndex

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
	bm.mu.Lock()
	defer bm.mu.Unlock()

	pi, err := bm.ensurePackIndexesLoaded()
	if err != nil {
		return 0, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := pi[blockID]
	if ndx == nil {
		return 0, storage.ErrBlockNotFound
	}

	return int64(ndx.Items[blockID].size), nil
}

func (bm *Manager) blockIDToPackSection(blockID string) (Info, bool, error) {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return Info{}, false, nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	pi, err := bm.ensurePackIndexesLoaded()
	if err != nil {
		return Info{}, false, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := pi[blockID]
	if ndx == nil {
		return Info{}, false, nil
	}

	if ndx.PackBlockID == "" {
		return Info{}, false, nil
	}

	if ndx.PackBlockID != "" && ndx.PackBlockID == blockID {
		// this is possible for a single-element pack
		return Info{}, false, nil
	}

	if blk, ok := ndx.Items[blockID]; ok {
		return Info{
			PackBlockID: ndx.PackBlockID,
			PackOffset:  int64(blk.offset),
			Length:      int64(blk.size),
		}, true, nil
	}

	return Info{}, false, fmt.Errorf("invalid pack index for %q", blockID)
}

func (bm *Manager) registerUnpackedBlock(packGroupID string, blockID string, dataLength int64) error {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	g := bm.registerUnpackedBlockLockedNoFlush(packGroupID, blockID, dataLength)

	if bm.timeNow().After(bm.flushPackIndexesAfter) || len(g.currentPackIndex.Items) > maxNonPackedBlocksPerPackIndex {
		if err := bm.finishPackAndMaybeFlushIndexes(g); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) registerUnpackedBlockLockedNoFlush(groupID string, blockID string, dataLength int64) *packInfo {
	g := bm.ensurePackGroupLocked(groupID, true)

	// See if we already have this block ID in an unpacked pack group.
	ndx, ok := bm.blockToIndex[blockID]
	if ok && ndx.PackGroup == groupID {
		return g
	}

	g.currentPackIndex.Items[blockID] = offsetAndSize{0, int32(dataLength)}
	bm.blockToIndex[blockID] = g.currentPackIndex
	return g
}

func (bm *Manager) addToPack(packGroup string, blockID string, data []byte) error {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return fmt.Errorf("pack objects can't be packed: %v", blockID)
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.ensurePackIndexesLoaded()

	// See if we already have this block ID in some pack.
	if _, ok := bm.blockToIndex[blockID]; ok {
		return nil
	}

	g := bm.ensurePackGroupLocked(packGroup, false)

	offset := len(g.currentPackData)
	shouldFinish := offset+len(data) >= bm.maxPackSize

	g.currentPackData = append(g.currentPackData, data...)
	g.currentPackIndex.Items[blockID] = offsetAndSize{int32(offset), int32(len(data))}
	bm.blockToIndex[blockID] = g.currentPackIndex

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
	g := bm.openPackGroups[packGroup]
	if g == nil {
		g = &packInfo{}
		bm.openPackGroups[packGroup] = g
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

	return bm.writeUnpackedBlock(buf.Bytes(), packObjectPrefix, true)
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

	if dataLength := len(g.currentPackData); !isNonPacked(g.currentPackIndex.PackGroup) {
		blockID, err := bm.writeUnpackedBlock(g.currentPackData, "", true)
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

func isNonPacked(g string) bool {
	switch g {
	case nonPackedObjectsPackGroup, packObjectsPackGroup, legacyUnpackedObjectsPackGroup:
		return true
	default:
		return false
	}
}

func (bm *Manager) loadMergedPackIndexLocked(cutoffTime time.Time) (packIndexes, []string, error) {
	ch, cancel := bm.storage.ListBlocks(packObjectPrefix)
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

				data, err := bm.getBlockInternal(b.BlockID)
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

func (bm *Manager) ensurePackIndexesLoaded() (map[string]*packIndex, error) {
	pi := bm.blockToIndex
	if pi != nil {
		return pi, nil
	}

	t0 := time.Now()

	merged, _, err := bm.loadMergedPackIndexLocked(bm.timeNow().Add(24 * time.Hour))
	if err != nil {
		return nil, err
	}

	bm.blockToIndex = dedupeBlockIDsAndIndex(merged)

	log.Printf("loaded %v indexes of %v blocks in %v", len(merged), len(bm.blockToIndex), time.Since(t0))

	return bm.blockToIndex, nil
}

func dedupeBlockIDsAndIndex(ndx packIndexes) map[string]*packIndex {
	pi := make(map[string]*packIndex)
	for _, pck := range ndx {
		for blockID := range pck.Items {
			if o := pi[blockID]; o != nil {
				if !pck.CreateTime.Before(o.CreateTime) {
					// this pack is same or newer.
					delete(o.Items, blockID)
					pi[blockID] = pck
				} else {
					// this pack is older.
					delete(pck.Items, blockID)
				}
			} else {
				pi[blockID] = pck
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

// CompactIndexes performs compaction of index blocks and optionally removes index blocks not present in the provided set.
func (bm *Manager) CompactIndexes(cutoffTime time.Time, inUseBlocks map[string]bool) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

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
	bm.mu.Lock()
	defer bm.mu.Unlock()

	var result []Info

	bm.ensurePackIndexesLoaded()

	packBlockIDs := map[string]bool{}
	for _, b := range bm.blockToIndex {
		packBlockIDs[b.PackBlockID] = true
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
			return ndx.PackGroup != legacyUnpackedObjectsPackGroup
		}

	case "nonpacked": // blocks that are not packed
		blockMatches = func(b Info, ndx *packIndex) bool {
			return ndx.PackGroup == legacyUnpackedObjectsPackGroup
		}

	default:
		blockMatches = func(Info, *packIndex) bool { return false }
	}

	for b, ndx := range bm.blockToIndex {
		if !strings.HasPrefix(b, prefix) {
			continue
		}

		bm := Info{
			BlockID:     b,
			Length:      int64(ndx.Items[b].size),
			Timestamp:   ndx.CreateTime,
			PackGroup:   ndx.PackGroup,
			PackBlockID: ndx.PackBlockID,
			PackOffset:  int64(ndx.Items[b].offset),
		}

		if !blockMatches(bm, ndx) {
			continue
		}

		result = append(result, bm)
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
	bm.mu.Lock()
	defer bm.mu.Unlock()

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
func (bm *Manager) WriteBlock(packGroup string, data []byte, prefix string) (string, error) {
	if bm.maxPackedContentLength > 0 && len(data) <= bm.maxPackedContentLength {
		blockID := prefix + bm.hashData(data)

		err := bm.addToPack(packGroup, blockID, data)
		return blockID, err
	}

	blockID, err := bm.writeUnpackedBlock(data, prefix, false)
	if err != nil {
		return "", err
	}

	bm.registerUnpackedBlock(nonPackedObjectsPackGroup, blockID, int64(len(data)))
	return blockID, nil
}

func (bm *Manager) writeUnpackedBlock(data []byte, prefix string, force bool) (string, error) {
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
	_, err := bm.ensurePackIndexesLoaded()
	if err != nil {
		return nil, fmt.Errorf("can't load pack index: %v", err)
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	if b, err := bm.getPendingBlockLocked(blockID); err == nil {
		return b, nil
	}

	return bm.getBlockInternal(blockID)
}

// BlockInfo returns information about a single block.
func (bm *Manager) BlockInfo(blockID string) (Info, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	_, err := bm.ensurePackIndexesLoaded()
	if err != nil {
		return Info{}, fmt.Errorf("can't load pack index: %v", err)
	}

	return bm.blockInfoLocked(blockID)
}

func (bm *Manager) blockInfoLocked(blockID string) (Info, error) {
	ndx := bm.blockToIndex[blockID]
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

func (bm *Manager) getBlockInternal(blockID string) ([]byte, error) {
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
