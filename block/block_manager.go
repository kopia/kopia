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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"
	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
)

const (
	parallelFetches              = 5                // number of parallel reads goroutines
	flushPackIndexTimeout        = 10 * time.Minute // time after which all pending indexes are flushes
	indexBlockPrefix             = "i"              // prefix for all storage blocks that are pack indexes
	compactedBlockSuffix         = "-z"
	defaultMinPreambleLength     = 32
	defaultMaxPreambleLength     = 32
	defaultPaddingUnit           = 4096
	maxInlineContentLength       = 100000 // amount of block data to store in the index block itself
	autoCompactionBlockCount     = 16
	defaultActiveBlocksExtraTime = 10 * time.Minute

	currentWriteVersion     = 1
	minSupportedReadVersion = 0
	maxSupportedReadVersion = currentWriteVersion
)

// Info is an information about a single block managed by Manager.
type Info struct {
	BlockID        ContentID       `json:"blockID"`
	Length         uint32          `json:"length"`
	TimestampNanos int64           `json:"time"`
	PackBlockID    PhysicalBlockID `json:"packBlockID,omitempty"`
	PackOffset     uint32          `json:"packOffset,omitempty"`
	Deleted        bool            `json:"deleted"`
	Payload        []byte          `json:"payload"` // set for payloads stored inline
	FormatVersion  int32           `json:"formatVersion"`
}

func (i Info) Timestamp() time.Time {
	return time.Unix(0, i.TimestampNanos)
}

// ContentID uniquely identifies a block of content stored in repository.
// It consists of optional one-character prefix (which can't be 0..9 or a..f) followed by hexa-decimal
// digits representing hash of the content.
type ContentID string

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

	committedBlocks committedBlockIndex

	pendingBlocks      map[ContentID]Info // maps block ID to corresponding info
	pendingPackIndexes []packIndex

	currentPackDataLength int              // length of the current block
	currentPackIndex      packIndexBuilder // index of a current block

	flushPackIndexesAfter time.Time // time when those indexes should be flushed
	activeBlocksExtraTime time.Duration

	writeFormatVersion int32 // format version to write

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

	// We have this block in current pack index and it's already deleted there.
	if bi, ok := bm.pendingBlocks[blockID]; ok && bi.Deleted {
		return nil
	}

	// We have this block in current pack index and it's already deleted there.
	if bi, err := bm.committedBlocks.getBlock(blockID); err == nil && bi.Deleted {
		return nil
	}

	// Add deletion to current pack.
	bm.currentPackIndex.deleteBlock(blockID)
	bm.pendingBlocks[blockID] = Info{
		BlockID:        blockID,
		Deleted:        true,
		TimestampNanos: bm.currentPackIndex.createTimeNanos(),
	}
	return nil
}

func (bm *Manager) addToPackLocked(ctx context.Context, blockID ContentID, data []byte) error {
	bm.assertLocked()

	bm.currentPackDataLength += len(data)
	shouldFinish := bm.currentPackDataLength >= bm.maxPackSize

	bm.currentPackIndex.addInlineBlock(blockID, data)
	bm.pendingBlocks[blockID] = Info{
		BlockID:        blockID,
		Payload:        data,
		Length:         uint32(len(data)),
		TimestampNanos: bm.currentPackIndex.createTimeNanos(),
	}

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
}

func (bm *Manager) startPackIndexLocked() {
	bm.currentPackIndex = newPackIndexV1(bm.timeNow())
	bm.currentPackDataLength = 0
}

func (bm *Manager) flushPackIndexesLocked(ctx context.Context) error {
	if len(bm.pendingPackIndexes) > 0 {
		if _, err := bm.writePackIndexes(ctx, bm.pendingPackIndexes, false); err != nil {
			return err
		}
		bm.pendingPackIndexes = nil
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)
	bm.committedBlocks.commit("", bm.pendingBlocks)
	return nil
}

func (bm *Manager) writePackIndexes(ctx context.Context, ndx []packIndex, isCompaction bool) (PhysicalBlockID, error) {
	pb := &blockmgrpb.Indexes{}

	for _, n := range ndx {
		n.addToIndexes(pb)
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		return "", fmt.Errorf("can't encode pack index: %v", err)
	}

	var suffix string
	if isCompaction {
		suffix = compactedBlockSuffix
	}

	inverseTimePrefix := fmt.Sprintf("%016x", math.MaxInt64-time.Now().UnixNano())
	return bm.encryptAndWriteBlockNotLocked(ctx, data, indexBlockPrefix+inverseTimePrefix, suffix)
}

func (bm *Manager) finishPackLocked(ctx context.Context) error {
	if isIndexEmpty(bm.currentPackIndex) {
		return nil
	}

	if bm.currentPackDataLength > 0 && bm.currentPackDataLength > bm.maxInlineContentLength {
		if err := bm.writePackBlock(ctx); err != nil {
			return fmt.Errorf("error writing pack block: %v", err)
		}
	}

	bm.pendingPackIndexes = append(bm.pendingPackIndexes, bm.currentPackIndex)
	bm.startPackIndexLocked()
	return nil
}

func (bm *Manager) writePackBlock(ctx context.Context) error {
	blockData, err := appendRandomBytes(nil, rand.Intn(bm.maxPreambleLength-bm.minPreambleLength+1)+bm.minPreambleLength)
	if err != nil {
		return err
	}

	items := bm.currentPackIndex.clearInlineBlocks()
	for blockID, data := range items {
		encrypted, encerr := bm.maybeEncryptBlockDataForPacking(data, blockID)
		if encerr != nil {
			return fmt.Errorf("unable to encrypt %q: %v", blockID, err)
		}
		bm.currentPackIndex.addPackedBlock(blockID, uint32(len(blockData)), uint32(len(data)))
		blockData = append(blockData, encrypted...)
	}

	if bm.paddingUnit > 0 {
		if missing := bm.paddingUnit - (len(blockData) % bm.paddingUnit); missing > 0 {
			blockData, err = appendRandomBytes(blockData, missing)
			if err != nil {
				return err
			}
		}
	}

	packBlockID, err := bm.writePackDataNotLocked(ctx, blockData)
	if err != nil {
		return fmt.Errorf("can't save pack data block %q: %v", packBlockID, err)
	}

	bm.currentPackIndex.finishPack(packBlockID, uint32(len(blockData)), bm.writeFormatVersion)
	return nil
}

func (bm *Manager) maybeEncryptBlockDataForPacking(data []byte, blockID ContentID) ([]byte, error) {
	if bm.writeFormatVersion == 0 {
		// in v0 the entire block is encrypted together later on
		return data, nil
	}
	iv, err := getPackedBlockIV(blockID)
	if err != nil {
		return nil, fmt.Errorf("unable to get packed block IV for %q: %v", blockID, err)
	}
	return bm.formatter.Encrypt(data, iv, 0)
}

func appendRandomBytes(b []byte, count int) ([]byte, error) {
	rnd := make([]byte, count)
	if _, err := io.ReadFull(cryptorand.Reader, rnd); err != nil {
		return nil, err
	}

	return append(b, rnd...), nil
}

// ListIndexBlocks returns the list of all index blocks, including inactive, sorted by time.
func (bm *Manager) ListIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	blocks, err := bm.cache.listIndexBlocks(ctx, true, 0)
	if err != nil {
		return nil, fmt.Errorf("error listing index blocks: %v", err)
	}

	sortBlocksByTime(blocks)
	return blocks, nil
}

// ActiveIndexBlocks returns the list of active index blocks, sorted by time.
func (bm *Manager) ActiveIndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	blocks, err := bm.cache.listIndexBlocks(ctx, false, bm.activeBlocksExtraTime)
	if err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, nil
	}

	sortBlocksByTime(blocks)
	return blocks, nil
}

func sortBlocksByTime(b []IndexInfo) {
	sort.Slice(b, func(i, j int) bool {
		return b[i].Timestamp.Before(b[j].Timestamp)
	})
}

func (bm *Manager) loadPackIndexesLocked(ctx context.Context) ([]PhysicalBlockID, error) {
	log.Debug().Msg("listing active index blocks")
	blocks, err := bm.ActiveIndexBlocks(ctx)
	if err != nil {
		return nil, err
	}

	var indexBlockIDs []PhysicalBlockID

	// add block IDs to the channel
	ch := make(chan PhysicalBlockID, len(blocks))
	for _, b := range blocks {
		indexBlockIDs = append(indexBlockIDs, b.BlockID)

		has, err := bm.committedBlocks.hasIndexBlockID(b.BlockID)
		if err != nil {
			return nil, err
		}

		if has {
			log.Printf("index block %q already in cache, skipping", b.BlockID)
			continue
		}

		ch <- b.BlockID
	}
	close(ch)
	if len(ch) == 0 {
		return indexBlockIDs, nil
	}

	log.Debug().Int("parallelism", parallelFetches).Int("count", len(ch)).Msg("loading active blocks")

	var wg sync.WaitGroup

	errors := make(chan error, parallelFetches)

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for indexBlockID := range ch {
				data, err := bm.getPhysicalBlockInternal(ctx, indexBlockID)
				if err != nil {
					errors <- err
					return
				}

				if err := bm.loadPackIndexes(indexBlockID, data); err != nil {
					errors <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Propagate async errors, if any.
	for err := range errors {
		return nil, err
	}

	return indexBlockIDs, nil
}

func (bm *Manager) loadPackIndexes(indexBlockID PhysicalBlockID, data []byte) error {
	var b blockmgrpb.Indexes

	if err := proto.Unmarshal(data, &b); err != nil {
		return err
	}

	var result []packIndex
	for _, ndx := range b.IndexesV1 {
		result = append(result, protoPackIndexV1{ndx})
	}

	_, err := bm.committedBlocks.load(indexBlockID, result)
	if err != nil {
		return fmt.Errorf("unable to add to committed block cache: %v", err)
	}

	return nil
}

func (bm *Manager) initializeIndexes(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	indexBlocks, err := bm.loadPackIndexesLocked(ctx)
	if err != nil {
		return err
	}
	log.Debug().Msgf("loaded %v index blocks", len(indexBlocks))

	// if len(indexBlocks) >= autoCompactionBlockCount {
	// 	log.Debug().Msgf("auto compacting block indexes (block count %v exceeds threshold of %v)", len(indexBlocks), autoCompactionBlockCount)
	// 	if _, err := bm.writePackIndexes(ctx, mergeIndexes(indexBlocks), true); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func mergeIndexes(m map[PhysicalBlockID][]packIndex) []packIndex {
	var result []packIndex

	for _, v := range m {
		result = append(result, v...)
	}
	return result
}

// CompactIndexes performs compaction of index blocks.
func (bm *Manager) CompactIndexes(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	// if err := bm.compactIndexes(ctx, indexes); err != nil {
	// 	return err
	// }

	return nil
}

// ListBlocks returns the metadata about blocks with a given prefix and kind.
func (bm *Manager) ListBlocks(prefix ContentID) ([]Info, error) {
	bm.lock()
	defer bm.unlock()

	var result []Info

	appendToResult := func(i Info) error {
		if i.Deleted || !strings.HasPrefix(string(i.BlockID), string(prefix)) {
			return nil
		}
		result = append(result, i)
		return nil
	}

	for _, ndx := range bm.pendingPackIndexes {
		_ = ndx.iterate(appendToResult)
	}

	_ = bm.committedBlocks.listBlocks(prefix, appendToResult)

	return result, nil
}

func (bm *Manager) compactIndexes(ctx context.Context, indexes map[PhysicalBlockID][]packIndex) error {
	if len(indexes) <= 1 {
		log.Printf("skipping index compaction - already compacted")
		return nil
	}

	_, err := bm.writePackIndexes(ctx, mergeIndexes(indexes), true)
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
	if err := validatePrefix(prefix); err != nil {
		return "", err
	}
	blockID := prefix + ContentID(hex.EncodeToString(bm.hashData(data)))

	bm.lock()
	defer bm.unlock()

	// See if we already have this block ID in some pack index and it's not deleted.
	if bi, ok := bm.pendingBlocks[blockID]; ok && !bi.Deleted {
		return blockID, nil
	}

	if bi, err := bm.committedBlocks.getBlock(blockID); err == nil && !bi.Deleted {
		return blockID, nil
	}

	err := bm.addToPackLocked(ctx, blockID, data)
	return blockID, err
}

func validatePrefix(prefix ContentID) error {
	switch len(prefix) {
	case 0:
		return nil
	case 1:
		if prefix[0] >= 'g' && prefix[0] <= 'z' {
			return nil
		}
	}

	return fmt.Errorf("invalid prefix, must be a empty or single letter between 'g' and 'z'")
}

func (bm *Manager) writePackDataNotLocked(ctx context.Context, data []byte) (PhysicalBlockID, error) {
	if bm.writeFormatVersion == 0 {
		// 0 blocks are encrypted together
		return bm.encryptAndWriteBlockNotLocked(ctx, data, "", "")
	}

	suffix := make([]byte, 16)
	if _, err := cryptorand.Read(suffix); err != nil {
		return "", fmt.Errorf("unable to read crypto bytes: %v", err)
	}

	physicalBlockID := PhysicalBlockID(fmt.Sprintf("%v-%x", time.Now().UTC().Format("20060102"), suffix))

	atomic.AddInt32(&bm.stats.WrittenBlocks, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data)))
	if err := bm.cache.putBlock(ctx, physicalBlockID, data); err != nil {
		return "", err
	}

	return physicalBlockID, nil
}

func (bm *Manager) encryptAndWriteBlockNotLocked(ctx context.Context, data []byte, prefix string, suffix string) (PhysicalBlockID, error) {
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
		bi, err := bm.currentPackIndex.getBlock(blockID)
		if err == nil {
			if bi.Deleted {
				return nil, storage.ErrBlockNotFound
			}

			if bi.Payload != nil {
				return bi.Payload, nil
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

	return bm.getPhysicalBlockInternal(ctx, blockID)
}

// BlockInfo returns information about a single block.
func (bm *Manager) BlockInfo(ctx context.Context, blockID ContentID) (Info, error) {
	bm.lock()
	defer bm.unlock()

	return bm.packedBlockInfoLocked(blockID)
}

func (bm *Manager) packedBlockInfoLocked(blockID ContentID) (Info, error) {
	bm.assertLocked()

	if bi, ok := bm.pendingBlocks[blockID]; ok {
		return bi, nil
	}

	return bm.committedBlocks.getBlock(blockID)
}

func (bm *Manager) getPackedBlockInternalLocked(ctx context.Context, blockID ContentID) ([]byte, error) {
	bm.assertLocked()

	bi, err := bm.packedBlockInfoLocked(blockID)
	if err != nil || bi.Deleted {
		return nil, storage.ErrBlockNotFound
	}

	// block stored inline
	if bi.Payload != nil {
		return bi.Payload, nil
	}

	packBlockID := bi.PackBlockID
	payload, err := bm.cache.getBlock(ctx, string(blockID), packBlockID, int64(bi.PackOffset), int64(bi.Length))
	if err != nil {
		return nil, err
	}

	return bm.decryptAndVerifyPayload(bi.FormatVersion, payload, int(bi.PackOffset), blockID, packBlockID)
}

func (bm *Manager) decryptAndVerifyPayload(formatVersion int32, payload []byte, offset int, blockID ContentID, packBlockID PhysicalBlockID) ([]byte, error) {
	atomic.AddInt32(&bm.stats.ReadBlocks, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	var err error
	var decryptIV, verifyIV []byte
	var decryptOffset int

	if formatVersion == 0 {
		decryptOffset = offset
		decryptIV, err = getPhysicalBlockIV(packBlockID)
		if err != nil {
			return nil, err
		}
		verifyIV, err = getPackedBlockIV(blockID)
		if err != nil {
			return nil, err
		}
	} else {
		decryptIV, err = getPackedBlockIV(blockID)
		verifyIV = decryptIV
		if err != nil {
			return nil, err
		}
	}

	payload, err = bm.formatter.Decrypt(payload, decryptIV, decryptOffset)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&bm.stats.DecryptedBytes, int64(len(payload)))

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := bm.verifyChecksum(payload, verifyIV); err != nil {
		return nil, err
	}

	return payload, nil
}

func (bm *Manager) getPhysicalBlockInternal(ctx context.Context, blockID PhysicalBlockID) ([]byte, error) {
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
func listIndexBlocksFromStorage(ctx context.Context, st storage.Storage, full bool, extraTime time.Duration) ([]IndexInfo, error) {
	maxCompactions := 1
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

		if strings.Contains(string(ii.BlockID), compactedBlockSuffix) {
			numCompactions++
			if numCompactions == maxCompactions {
				timestampCutoff = it.TimeStamp.Add(-extraTime)
			}
		}
	}

	return results, nil
}

// NewManager creates new block manager with given packing options and a formatter.
func NewManager(ctx context.Context, st storage.Storage, f FormattingOptions, caching CachingOptions) (*Manager, error) {
	return newManagerWithOptions(ctx, st, f, caching, time.Now, defaultActiveBlocksExtraTime)
}

func newManagerWithOptions(ctx context.Context, st storage.Storage, f FormattingOptions, caching CachingOptions, timeNow func() time.Time, activeBlocksExtraTime time.Duration) (*Manager, error) {
	if f.Version < minSupportedReadVersion || f.Version > currentWriteVersion {
		return nil, fmt.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", f.Version, minSupportedReadVersion, maxSupportedReadVersion)
	}
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

	var cbi committedBlockIndex
	if caching.CacheDirectory != "" {
		cbi, err = newLevelDBCommittedBlockIndex(filepath.Join(caching.CacheDirectory, "index"))
		if err != nil {
			return nil, fmt.Errorf("unable to initialize block index cache: %v", err)
		}
	} else {
		cbi = newCommittedBlockIndex()
	}

	m := &Manager{
		Format:                 f,
		timeNow:                timeNow,
		flushPackIndexesAfter:  timeNow().Add(flushPackIndexTimeout),
		maxPackSize:            f.MaxPackSize,
		formatter:              formatter,
		pendingBlocks:          make(map[ContentID]Info),
		committedBlocks:        cbi,
		minPreambleLength:      defaultMinPreambleLength,
		maxPreambleLength:      defaultMaxPreambleLength,
		paddingUnit:            defaultPaddingUnit,
		maxInlineContentLength: maxInlineContentLength,
		cache: cache,
		activeBlocksExtraTime: activeBlocksExtraTime,
		writeFormatVersion:    int32(f.Version),
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
