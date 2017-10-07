package repo

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

	"github.com/kopia/kopia/blob"
)

const flushPackIndexTimeout = 10 * time.Minute
const packObjectPrefix = "P"
const unpackedObjectsPackGroup = "_unpacked_"
const maxNonPackedBlocksPerPackIndex = 200

type packInfo struct {
	currentPackData  []byte
	currentPackIndex *packIndex
}

type blockLocation struct {
	packIndex   int
	objectIndex int
}

type blockManager struct {
	storage blob.Storage
	stats   Stats

	mu           sync.Mutex
	blockToIndex map[string]*packIndex

	pendingPackIndexes    packIndexes
	flushPackIndexesAfter time.Time

	openPackGroups         map[string]*packInfo
	maxPackedContentLength int
	maxPackSize            int
	formatter              objectFormatter

	timeNow func() time.Time
}

func (bm *blockManager) BlockSize(blockID string) (int64, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	pi, err := bm.ensurePackIndexesLoaded()
	if err != nil {
		return 0, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := pi[blockID]
	if ndx == nil {
		return 0, blob.ErrBlockNotFound
	}

	return int64(ndx.Items[blockID].size), nil
}

func (bm *blockManager) blockIDToPackSection(blockID string) (ObjectIDSection, bool, error) {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return ObjectIDSection{}, false, nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	pi, err := bm.ensurePackIndexesLoaded()
	if err != nil {
		return ObjectIDSection{}, false, fmt.Errorf("can't load pack index: %v", err)
	}

	ndx := pi[blockID]
	if ndx == nil {
		return ObjectIDSection{}, false, nil
	}

	if ndx.PackBlockID == "" {
		return ObjectIDSection{}, false, nil
	}

	if ndx.PackBlockID != "" && ndx.PackBlockID == blockID {
		// this is possible for a single-element pack
		return ObjectIDSection{}, false, nil
	}

	if blk, ok := ndx.Items[blockID]; ok {
		return ObjectIDSection{
			Base:   ObjectID{StorageBlock: ndx.PackBlockID},
			Start:  int64(blk.offset),
			Length: int64(blk.size),
		}, true, nil
	}

	return ObjectIDSection{}, false, fmt.Errorf("invalid pack index for %q", blockID)
}

func (bm *blockManager) registerUnpackedBlock(blockID string, dataLength int64) error {
	if strings.HasPrefix(blockID, packObjectPrefix) {
		return nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	g := bm.registerUnpackedBlockLockedNoFlush(blockID, dataLength)

	if bm.timeNow().After(bm.flushPackIndexesAfter) || len(g.currentPackIndex.Items) > maxNonPackedBlocksPerPackIndex {
		if err := bm.finishPackAndMaybeFlushIndexes(g); err != nil {
			return err
		}
	}

	return nil
}

func (bm *blockManager) registerUnpackedBlockLockedNoFlush(blockID string, dataLength int64) *packInfo {
	g := bm.ensurePackGroupLocked(unpackedObjectsPackGroup)

	// See if we already have this block ID in an unpacked pack group.
	ndx, ok := bm.blockToIndex[blockID]
	if ok && ndx.PackGroup == unpackedObjectsPackGroup {
		return g
	}

	g.currentPackIndex.Items[blockID] = offsetAndSize{0, int32(dataLength)}
	bm.blockToIndex[blockID] = g.currentPackIndex
	return g
}

func (bm *blockManager) addToPack(packGroup string, blockID string, data []byte) error {
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

	g := bm.ensurePackGroupLocked(packGroup)

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

func (bm *blockManager) finishPackAndMaybeFlushIndexes(g *packInfo) error {
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

func (bm *blockManager) ensurePackGroupLocked(packGroup string) *packInfo {
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
		g.currentPackData = g.currentPackData[:0]
	}

	return g
}

func (bm *blockManager) flushPackIndexesLocked() error {
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

func (bm *blockManager) writePackIndexes(ndx packIndexes) (string, error) {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(ndx); err != nil {
		return "", fmt.Errorf("can't encode pack index: %v", err)
	}
	zw.Close()

	blockID, err := bm.hashEncryptAndWrite("", buf.Bytes(), packObjectPrefix, true)
	if err != nil {
		return "", fmt.Errorf("can't save pack index object: %v", err)
	}

	return blockID, nil
}

func (bm *blockManager) finishAllOpenPacksLocked() error {
	// finish non-unpacked groups first.
	for _, g := range bm.openPackGroups {
		if g.currentPackIndex != nil && g.currentPackIndex.PackGroup != unpackedObjectsPackGroup {
			if err := bm.finishPackLocked(g); err != nil {
				return err
			}
		}
	}
	// finish unpacked groups next.
	for _, g := range bm.openPackGroups {
		if g.currentPackIndex != nil && g.currentPackIndex.PackGroup == unpackedObjectsPackGroup {
			if err := bm.finishPackLocked(g); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bm *blockManager) finishPackLocked(g *packInfo) error {
	if g.currentPackIndex == nil {
		return nil
	}

	if dataLength := len(g.currentPackData); g.currentPackIndex.PackGroup != unpackedObjectsPackGroup {
		blockID, err := bm.hashEncryptAndWrite(unpackedObjectsPackGroup, g.currentPackData, "", true)

		if err != nil {
			return fmt.Errorf("can't save pack data: %v", err)
		}

		bm.registerUnpackedBlockLockedNoFlush(blockID, int64(dataLength))
		g.currentPackIndex.PackBlockID = blockID
	}

	if len(g.currentPackIndex.Items) > 0 {
		bm.pendingPackIndexes = append(bm.pendingPackIndexes, g.currentPackIndex)
	}
	g.currentPackData = g.currentPackData[:0]
	g.currentPackIndex = nil

	return nil
}

func (bm *blockManager) loadMergedPackIndexLocked(olderThan *time.Time) (packIndexes, []string, error) {
	ch, cancel := bm.storage.ListBlocks(packObjectPrefix)
	defer cancel()

	t0 := time.Now()

	var wg sync.WaitGroup

	errors := make(chan error, parallelFetches)
	var mu sync.Mutex

	packIndexData := map[string][]byte{}
	totalSize := 0
	var blockIDs []string
	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for b := range ch {
				if b.Error != nil {
					errors <- b.Error
					return
				}

				if olderThan != nil && b.TimeStamp.After(*olderThan) {
					return
				}

				data, err := bm.getBlockInternal(b.BlockID)
				if err != nil {
					errors <- err
					return
				}

				mu.Lock()
				packIndexData[b.BlockID] = data
				blockIDs = append(blockIDs, b.BlockID)
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
		log.Printf("loaded %v pack indexes (%v bytes) in %v", len(packIndexData), totalSize, time.Since(t0))
	}

	var indexes []packIndexes

	for blockID, content := range packIndexData {
		var r io.Reader = bytes.NewReader(content)
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to read pack index from %q: %v", blockID, err)
		}

		pi, err := loadPackIndexes(zr)
		if err != nil {
			return nil, nil, err
		}

		indexes = append(indexes, pi)
	}

	topLevelBlocks := map[string]bool{}

	for _, pi := range indexes {
		for _, ndx := range pi {
			if ndx.PackGroup == unpackedObjectsPackGroup {
				for blockID := range ndx.Items {
					topLevelBlocks[blockID] = true
				}
			}
		}
	}

	for _, pi := range indexes {
		for _, ndx := range pi {
			if ndx.PackGroup != unpackedObjectsPackGroup {
				if !topLevelBlocks[ndx.PackBlockID] {
					log.Printf("warning: pack block %q (%v) not found", ndx.PackBlockID, ndx.CreateTime)
				}
			}
		}
	}

	var merged packIndexes
	for _, pi := range indexes {
		merged = append(merged, pi...)
	}

	return merged, blockIDs, nil
}

func (bm *blockManager) ensurePackIndexesLoaded() (map[string]*packIndex, error) {
	pi := bm.blockToIndex
	if pi != nil {
		return pi, nil
	}

	t0 := time.Now()

	merged, _, err := bm.loadMergedPackIndexLocked(nil)
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

func (bm *blockManager) Compact(cutoffTime time.Time, inUseBlocks map[string]bool) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	merged, blockIDs, err := bm.loadMergedPackIndexLocked(&cutoffTime)
	if err != nil {
		return err
	}

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

func (bm *blockManager) Flush() error {
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

func (bm *blockManager) WriteBlock(packGroup string, data []byte, prefix string) (string, error) {
	return bm.hashEncryptAndWrite(packGroup, data, prefix, false)
}

// hashEncryptAndWrite computes hash of a given buffer, optionally encrypts and writes it to storage.
// The write is not guaranteed to complete synchronously in case write-back is used, but by the time
// Repository.Close() returns all writes are guaranteed be over.
func (bm *blockManager) hashEncryptAndWrite(packGroup string, data []byte, prefix string, isPackInternalObject bool) (string, error) {
	// Hash the block and compute encryption key.
	blockID := prefix + bm.formatter.ComputeBlockID(data)
	atomic.AddInt32(&bm.stats.HashedBlocks, 1)
	atomic.AddInt64(&bm.stats.HashedBytes, int64(len(data)))

	if !isPackInternalObject {
		if bm.maxPackedContentLength > 0 && len(data) <= bm.maxPackedContentLength {
			err := bm.addToPack(packGroup, blockID, data)
			return blockID, err
		}

		// Before performing encryption, check if the block is already there.
		blockSize, err := bm.BlockSize(blockID)
		atomic.AddInt32(&bm.stats.CheckedBlocks, int32(1))
		if err == nil && blockSize == int64(len(data)) {
			atomic.AddInt32(&bm.stats.PresentBlocks, int32(1))
			// Block already exists in storage, correct size, return without uploading.
			return blockID, nil
		}

		if err != nil && err != blob.ErrBlockNotFound {
			// Don't know whether block exists in storage.
			return "", err
		}
	}

	// Encrypt the block in-place.
	atomic.AddInt64(&bm.stats.EncryptedBytes, int64(len(data)))
	data, err := bm.formatter.Encrypt(data, blockID, 0)
	if err != nil {
		return "", err
	}

	atomic.AddInt32(&bm.stats.WrittenBlocks, int32(1))
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data)))

	if err := bm.storage.PutBlock(blockID, data); err != nil {
		return "", err
	}

	if !isPackInternalObject {
		bm.registerUnpackedBlock(blockID, int64(len(data)))
	}
	return blockID, nil
}

func (bm *blockManager) getPendingBlock(blockID string) ([]byte, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, p := range bm.openPackGroups {
		if ndx := p.currentPackIndex; ndx != nil {
			if ndx.PackGroup == unpackedObjectsPackGroup {
				continue
			}
			if blk, ok := ndx.Items[blockID]; ok {
				return p.currentPackData[blk.offset : blk.offset+blk.size], nil
			}
		}
	}
	return nil, blob.ErrBlockNotFound
}

func (bm *blockManager) GetBlock(blockID string) ([]byte, error) {
	if b, err := bm.getPendingBlock(blockID); err == nil {
		return b, nil
	}

	return bm.getBlockInternal(blockID)
}

func (bm *blockManager) getBlockInternal(blockID string) ([]byte, error) {
	s, ok, err := bm.blockIDToPackSection(blockID)
	if err != nil {
		if err != blob.ErrBlockNotFound {
			return nil, err
		}

	}

	var payload []byte
	underlyingBlockID := blockID
	var decryptSkip int

	if ok {
		underlyingBlockID = s.Base.StorageBlock
		payload, err = bm.storage.GetBlock(underlyingBlockID, s.Start, s.Length)
		decryptSkip = int(s.Start)
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

func (bm *blockManager) verifyChecksum(data []byte, blockID string) error {
	expected := bm.formatter.ComputeBlockID(data)
	if !strings.HasSuffix(blockID, expected) {
		atomic.AddInt32(&bm.stats.InvalidBlocks, 1)
		return fmt.Errorf("invalid checksum for blob: '%v', expected %v", blockID, expected)
	}

	atomic.AddInt32(&bm.stats.ValidBlocks, 1)
	return nil
}

func newBlockManager(storage blob.Storage, maxPackedContentLength, maxPackSize int, formatter objectFormatter) *blockManager {
	return &blockManager{
		storage:                storage,
		openPackGroups:         make(map[string]*packInfo),
		timeNow:                time.Now,
		flushPackIndexesAfter:  time.Now().Add(flushPackIndexTimeout),
		pendingPackIndexes:     nil,
		maxPackedContentLength: maxPackedContentLength,
		maxPackSize:            maxPackSize,
		formatter:              formatter,
	}
}
