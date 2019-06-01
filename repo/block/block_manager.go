// Package block implements repository support content-addressable storage blocks.
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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/repo/blob"
)

var (
	log       = repologging.Logger("kopia/block")
	formatLog = repologging.Logger("kopia/block/format")
)

// PackBlobIDPrefix is the prefix for all pack blobs.
const PackBlobIDPrefix = "p"

const (
	parallelFetches          = 5                // number of parallel reads goroutines
	flushPackIndexTimeout    = 10 * time.Minute // time after which all pending indexes are flushes
	newIndexBlockPrefix      = "n"
	defaultMinPreambleLength = 32
	defaultMaxPreambleLength = 32
	defaultPaddingUnit       = 4096

	currentWriteVersion     = 1
	minSupportedReadVersion = 0
	maxSupportedReadVersion = currentWriteVersion

	indexLoadAttempts = 10
)

// ErrBlockNotFound is returned when block is not found.
var ErrBlockNotFound = errors.New("block not found")

// IndexInfo is an information about a single index block managed by Manager.
type IndexInfo struct {
	BlobID    blob.ID
	Length    int64
	Timestamp time.Time
}

// Manager manages storage blocks at a low level with encryption, deduplication and packaging.
type Manager struct {
	Format FormattingOptions

	stats      Stats
	blockCache *blockCache
	listCache  *listCache
	st         blob.Storage

	mu                      sync.Mutex
	locked                  bool
	checkInvariantsOnUnlock bool

	currentPackItems      map[string]Info  // blocks that are in the pack block currently being built (all inline)
	currentPackDataLength int              // total length of all items in the current pack block
	packIndexBuilder      packIndexBuilder // blocks that are in index currently being built (current pack and all packs saved but not committed)
	committedBlocks       *committedBlockIndex

	disableIndexFlushCount int
	flushPackIndexesAfter  time.Time // time when those indexes should be flushed

	closed chan struct{}

	writeFormatVersion int32 // format version to write

	maxPackSize int
	hasher      HashFunc
	encryptor   Encryptor

	minPreambleLength int
	maxPreambleLength int
	paddingUnit       int
	timeNow           func() time.Time

	repositoryFormatBytes []byte
}

// DeleteBlock marks the given blockID as deleted.
//
// NOTE: To avoid race conditions only blocks that cannot be possibly re-created
// should ever be deleted. That means that contents of such blocks should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *Manager) DeleteBlock(blockID string) error {
	bm.lock()
	defer bm.unlock()

	log.Debugf("DeleteBlock(%q)", blockID)

	// We have this block in current pack index and it's already deleted there.
	if bi, ok := bm.packIndexBuilder[blockID]; ok {
		if !bi.Deleted {
			if bi.PackBlobID == "" {
				// added and never committed, just forget about it.
				delete(bm.packIndexBuilder, blockID)
				delete(bm.currentPackItems, blockID)
				return nil
			}

			// added and committed.
			bi2 := *bi
			bi2.Deleted = true
			bi2.TimestampSeconds = bm.timeNow().Unix()
			bm.setPendingBlock(bi2)
		}
		return nil
	}

	// We have this block in current pack index and it's already deleted there.
	bi, err := bm.committedBlocks.getBlock(blockID)
	if err != nil {
		return err
	}

	if bi.Deleted {
		// already deleted
		return nil
	}

	// object present but not deleted, mark for deletion and add to pending
	bi2 := bi
	bi2.Deleted = true
	bi2.TimestampSeconds = bm.timeNow().Unix()
	bm.setPendingBlock(bi2)
	return nil
}

func (bm *Manager) setPendingBlock(i Info) {
	bm.packIndexBuilder.Add(i)
	bm.currentPackItems[i.BlockID] = i
}

func (bm *Manager) addToPackLocked(ctx context.Context, blockID string, data []byte, isDeleted bool) error {
	bm.assertLocked()

	data = cloneBytes(data)
	bm.currentPackDataLength += len(data)
	bm.setPendingBlock(Info{
		Deleted:          isDeleted,
		BlockID:          blockID,
		Payload:          data,
		Length:           uint32(len(data)),
		TimestampSeconds: bm.timeNow().Unix(),
	})

	if bm.currentPackDataLength >= bm.maxPackSize {
		if err := bm.finishPackAndMaybeFlushIndexesLocked(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) finishPackAndMaybeFlushIndexesLocked(ctx context.Context) error {
	bm.assertLocked()
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

// DisableIndexFlush increments the counter preventing automatic index flushes.
func (bm *Manager) DisableIndexFlush() {
	bm.lock()
	defer bm.unlock()
	log.Debugf("DisableIndexFlush()")
	bm.disableIndexFlushCount++
}

// EnableIndexFlush decrements the counter preventing automatic index flushes.
// The flushes will be reenabled when the index drops to zero.
func (bm *Manager) EnableIndexFlush() {
	bm.lock()
	defer bm.unlock()
	log.Debugf("EnableIndexFlush()")
	bm.disableIndexFlushCount--
}

func (bm *Manager) verifyInvariantsLocked() {
	bm.assertLocked()

	bm.verifyCurrentPackItemsLocked()
	bm.verifyPackIndexBuilderLocked()
}

func (bm *Manager) verifyCurrentPackItemsLocked() {
	for k, cpi := range bm.currentPackItems {
		bm.assertInvariant(cpi.BlockID == k, "block ID entry has invalid key: %v %v", cpi.BlockID, k)
		bm.assertInvariant(cpi.Deleted || cpi.PackBlobID == "", "block ID entry has unexpected pack block ID %v: %v", cpi.BlockID, cpi.PackBlobID)
		bm.assertInvariant(cpi.TimestampSeconds != 0, "block has no timestamp: %v", cpi.BlockID)
		bi, ok := bm.packIndexBuilder[k]
		bm.assertInvariant(ok, "block ID entry not present in pack index builder: %v", cpi.BlockID)
		bm.assertInvariant(reflect.DeepEqual(*bi, cpi), "current pack index does not match pack index builder: %v", cpi, *bi)
	}
}

func (bm *Manager) verifyPackIndexBuilderLocked() {
	for k, cpi := range bm.packIndexBuilder {
		bm.assertInvariant(cpi.BlockID == k, "block ID entry has invalid key: %v %v", cpi.BlockID, k)
		if _, ok := bm.currentPackItems[cpi.BlockID]; ok {
			// ignore blocks also in currentPackItems
			continue
		}
		if cpi.Deleted {
			bm.assertInvariant(cpi.PackBlobID == "", "block can't be both deleted and have a pack block: %v", cpi.BlockID)
		} else {
			bm.assertInvariant(cpi.PackBlobID != "", "block that's not deleted must have a pack block: %+v", cpi)
			bm.assertInvariant(cpi.FormatVersion == byte(bm.writeFormatVersion), "block that's not deleted must have a valid format version: %+v", cpi)
		}
		bm.assertInvariant(cpi.TimestampSeconds != 0, "block has no timestamp: %v", cpi.BlockID)
	}
}

func (bm *Manager) assertInvariant(ok bool, errorMsg string, arg ...interface{}) {
	if ok {
		return
	}

	if len(arg) > 0 {
		errorMsg = fmt.Sprintf(errorMsg, arg...)
	}

	panic(errorMsg)
}

func (bm *Manager) startPackIndexLocked() {
	bm.currentPackItems = make(map[string]Info)
	bm.currentPackDataLength = 0
}

func (bm *Manager) flushPackIndexesLocked(ctx context.Context) error {
	bm.assertLocked()

	if bm.disableIndexFlushCount > 0 {
		log.Debugf("not flushing index because flushes are currently disabled")
		return nil
	}

	if len(bm.packIndexBuilder) > 0 {
		var buf bytes.Buffer

		if err := bm.packIndexBuilder.Build(&buf); err != nil {
			return errors.Wrap(err, "unable to build pack index")
		}

		data := buf.Bytes()
		dataCopy := append([]byte(nil), data...)

		indexBlobID, err := bm.writePackIndexesNew(ctx, data)
		if err != nil {
			return err
		}

		if err := bm.committedBlocks.addBlock(indexBlobID, dataCopy, true); err != nil {
			return errors.Wrap(err, "unable to add committed block")
		}
		bm.packIndexBuilder = make(packIndexBuilder)
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)
	return nil
}

func (bm *Manager) writePackIndexesNew(ctx context.Context, data []byte) (blob.ID, error) {
	return bm.encryptAndWriteBlockNotLocked(ctx, data, newIndexBlockPrefix)
}

func (bm *Manager) finishPackLocked(ctx context.Context) error {
	if len(bm.currentPackItems) == 0 {
		log.Debugf("no current pack entries")
		return nil
	}

	if err := bm.writePackBlockLocked(ctx); err != nil {
		return errors.Wrap(err, "error writing pack block")
	}

	bm.startPackIndexLocked()
	return nil
}

func (bm *Manager) writePackBlockLocked(ctx context.Context) error {
	bm.assertLocked()

	blockID := make([]byte, 16)
	if _, err := cryptorand.Read(blockID); err != nil {
		return errors.Wrap(err, "unable to read crypto bytes")
	}

	packFile := blob.ID(fmt.Sprintf("%v%x", PackBlobIDPrefix, blockID))

	blockData, packFileIndex, err := bm.preparePackDataBlock(packFile)
	if err != nil {
		return errors.Wrap(err, "error preparing data block")
	}

	if len(blockData) > 0 {
		if err := bm.writePackFileNotLocked(ctx, packFile, blockData); err != nil {
			return errors.Wrap(err, "can't save pack data block")
		}
	}

	formatLog.Debugf("wrote pack file: %v (%v bytes)", packFile, len(blockData))
	for _, info := range packFileIndex {
		bm.packIndexBuilder.Add(*info)
	}

	return nil
}

func (bm *Manager) preparePackDataBlock(packFile blob.ID) ([]byte, packIndexBuilder, error) {
	formatLog.Debugf("preparing block data with %v items", len(bm.currentPackItems))

	blockData, err := appendRandomBytes(append([]byte(nil), bm.repositoryFormatBytes...), rand.Intn(bm.maxPreambleLength-bm.minPreambleLength+1)+bm.minPreambleLength)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to prepare block preamble")
	}

	packFileIndex := packIndexBuilder{}
	for blockID, info := range bm.currentPackItems {
		if info.Payload == nil {
			continue
		}

		var encrypted []byte
		encrypted, err = bm.maybeEncryptBlockDataForPacking(info.Payload, info.BlockID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to encrypt %q", blockID)
		}

		formatLog.Debugf("adding %v length=%v deleted=%v", blockID, len(info.Payload), info.Deleted)

		packFileIndex.Add(Info{
			BlockID:          blockID,
			Deleted:          info.Deleted,
			FormatVersion:    byte(bm.writeFormatVersion),
			PackBlobID:       packFile,
			PackOffset:       uint32(len(blockData)),
			Length:           uint32(len(encrypted)),
			TimestampSeconds: info.TimestampSeconds,
		})

		blockData = append(blockData, encrypted...)
	}

	if len(packFileIndex) == 0 {
		return nil, nil, nil
	}

	if bm.paddingUnit > 0 {
		if missing := bm.paddingUnit - (len(blockData) % bm.paddingUnit); missing > 0 {
			blockData, err = appendRandomBytes(blockData, missing)
			if err != nil {
				return nil, nil, errors.Wrap(err, "unable to prepare block postamble")
			}
		}
	}

	origBlockLength := len(blockData)
	blockData, err = bm.appendPackFileIndexRecoveryData(blockData, packFileIndex)

	formatLog.Debugf("finished block %v bytes (%v bytes index)", len(blockData), len(blockData)-origBlockLength)
	return blockData, packFileIndex, err
}

func (bm *Manager) maybeEncryptBlockDataForPacking(data []byte, blockID string) ([]byte, error) {
	if bm.writeFormatVersion == 0 {
		// in v0 the entire block is encrypted together later on
		return data, nil
	}
	iv, err := getPackedBlockIV(blockID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get packed block IV for %q", blockID)
	}

	return bm.encryptor.Encrypt(data, iv)
}

func appendRandomBytes(b []byte, count int) ([]byte, error) {
	rnd := make([]byte, count)
	if _, err := io.ReadFull(cryptorand.Reader, rnd); err != nil {
		return nil, err
	}

	return append(b, rnd...), nil
}

// IndexBlocks returns the list of active index blocks.
func (bm *Manager) IndexBlocks(ctx context.Context) ([]IndexInfo, error) {
	return bm.listCache.listIndexBlocks(ctx)
}

func (bm *Manager) loadPackIndexesUnlocked(ctx context.Context) ([]IndexInfo, bool, error) {
	nextSleepTime := 100 * time.Millisecond

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		if i > 0 {
			bm.listCache.deleteListCache(ctx)
			log.Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		blocks, err := bm.listCache.listIndexBlocks(ctx)
		if err != nil {
			return nil, false, err
		}

		err = bm.tryLoadPackIndexBlocksUnlocked(ctx, blocks)
		if err == nil {
			var blockIDs []blob.ID
			for _, b := range blocks {
				blockIDs = append(blockIDs, b.BlobID)
			}
			var updated bool
			updated, err = bm.committedBlocks.use(blockIDs)
			if err != nil {
				return nil, false, err
			}
			return blocks, updated, nil
		}
		if err != blob.ErrBlobNotFound {
			return nil, false, err
		}
	}

	return nil, false, errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (bm *Manager) tryLoadPackIndexBlocksUnlocked(ctx context.Context, blocks []IndexInfo) error {
	ch, unprocessedIndexesSize, err := bm.unprocessedIndexBlocksUnlocked(blocks)
	if err != nil {
		return err
	}
	if len(ch) == 0 {
		return nil
	}

	log.Infof("downloading %v new index blocks (%v bytes)...", len(ch), unprocessedIndexesSize)
	var wg sync.WaitGroup

	errch := make(chan error, parallelFetches)

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for indexBlockID := range ch {
				data, err := bm.getPhysicalBlockInternal(ctx, indexBlockID)
				if err != nil {
					errch <- err
					return
				}

				if err := bm.committedBlocks.addBlock(indexBlockID, data, false); err != nil {
					errch <- errors.Wrap(err, "unable to add to committed block cache")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errch)

	// Propagate async errors, if any.
	for err := range errch {
		return err
	}
	log.Infof("Index blocks downloaded.")

	return nil
}

// unprocessedIndexBlocksUnlocked returns a closed channel filled with block IDs that are not in committedBlocks cache.
func (bm *Manager) unprocessedIndexBlocksUnlocked(blocks []IndexInfo) (<-chan blob.ID, int64, error) {
	var totalSize int64
	ch := make(chan blob.ID, len(blocks))
	for _, block := range blocks {
		has, err := bm.committedBlocks.cache.hasIndexBlockID(block.BlobID)
		if err != nil {
			return nil, 0, err
		}
		if has {
			log.Debugf("index block %q already in cache, skipping", block.BlobID)
			continue
		}
		ch <- block.BlobID
		totalSize += block.Length
	}
	close(ch)
	return ch, totalSize, nil
}

// Close closes the block manager.
func (bm *Manager) Close() {
	bm.blockCache.close()
	close(bm.closed)
}

// ListBlocks returns IDs of blocks matching given prefix.
func (bm *Manager) ListBlocks(prefix string) ([]string, error) {
	bm.lock()
	defer bm.unlock()

	var result []string

	appendToResult := func(i Info) error {
		if i.Deleted || !strings.HasPrefix(i.BlockID, prefix) {
			return nil
		}
		if bi, ok := bm.packIndexBuilder[i.BlockID]; ok && bi.Deleted {
			return nil
		}
		result = append(result, i.BlockID)
		return nil
	}

	for _, bi := range bm.packIndexBuilder {
		_ = appendToResult(*bi)
	}

	_ = bm.committedBlocks.listBlocks(prefix, appendToResult)
	return result, nil
}

// ListBlockInfos returns the metadata about blocks with a given prefix and kind.
func (bm *Manager) ListBlockInfos(prefix string, includeDeleted bool) ([]Info, error) {
	bm.lock()
	defer bm.unlock()

	var result []Info

	appendToResult := func(i Info) error {
		if (i.Deleted && !includeDeleted) || !strings.HasPrefix(i.BlockID, prefix) {
			return nil
		}
		if bi, ok := bm.packIndexBuilder[i.BlockID]; ok && bi.Deleted {
			return nil
		}
		result = append(result, i)
		return nil
	}

	for _, bi := range bm.packIndexBuilder {
		_ = appendToResult(*bi)
	}

	_ = bm.committedBlocks.listBlocks(prefix, appendToResult)

	return result, nil
}

// Flush completes writing any pending packs and writes pack indexes to the underlyign storage.
func (bm *Manager) Flush(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	if err := bm.finishPackLocked(ctx); err != nil {
		return errors.Wrap(err, "error writing pending block")
	}

	if err := bm.flushPackIndexesLocked(ctx); err != nil {
		return errors.Wrap(err, "error flushing indexes")
	}

	return nil
}

// RewriteBlock causes reads and re-writes a given block using the most recent format.
func (bm *Manager) RewriteBlock(ctx context.Context, blockID string) error {
	bi, err := bm.getBlockInfo(blockID)
	if err != nil {
		return err
	}

	data, err := bm.getBlockContentsUnlocked(ctx, bi)
	if err != nil {
		return err
	}

	bm.lock()
	defer bm.unlock()
	return bm.addToPackLocked(ctx, blockID, data, bi.Deleted)
}

// WriteBlock saves a given block of data to a pack group with a provided name and returns a blockID
// that's based on the contents of data written.
func (bm *Manager) WriteBlock(ctx context.Context, data []byte, prefix string) (string, error) {
	if err := validatePrefix(prefix); err != nil {
		return "", err
	}
	blockID := prefix + hex.EncodeToString(bm.hashData(data))

	// block already tracked
	if bi, err := bm.getBlockInfo(blockID); err == nil {
		if !bi.Deleted {
			return blockID, nil
		}
	}

	log.Debugf("WriteBlock(%q) - new", blockID)
	bm.lock()
	defer bm.unlock()
	err := bm.addToPackLocked(ctx, blockID, data, false)
	return blockID, err
}

func validatePrefix(prefix string) error {
	switch len(prefix) {
	case 0:
		return nil
	case 1:
		if prefix[0] >= 'g' && prefix[0] <= 'z' {
			return nil
		}
	}

	return errors.Errorf("invalid prefix, must be a empty or single letter between 'g' and 'z'")
}

func (bm *Manager) writePackFileNotLocked(ctx context.Context, packFile blob.ID, data []byte) error {
	atomic.AddInt32(&bm.stats.WrittenBlocks, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data)))
	bm.listCache.deleteListCache(ctx)
	return bm.st.PutBlob(ctx, packFile, data)
}

func (bm *Manager) encryptAndWriteBlockNotLocked(ctx context.Context, data []byte, prefix blob.ID) (blob.ID, error) {
	hash := bm.hashData(data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	// Encrypt the block in-place.
	atomic.AddInt64(&bm.stats.EncryptedBytes, int64(len(data)))
	data2, err := bm.encryptor.Encrypt(data, hash)
	if err != nil {
		return "", err
	}

	atomic.AddInt32(&bm.stats.WrittenBlocks, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data2)))
	bm.listCache.deleteListCache(ctx)
	if err := bm.st.PutBlob(ctx, blobID, data2); err != nil {
		return "", err
	}

	return blobID, nil
}

func (bm *Manager) hashData(data []byte) []byte {
	// Hash the block and compute encryption key.
	blockID := bm.hasher(data)
	atomic.AddInt32(&bm.stats.HashedBlocks, 1)
	atomic.AddInt64(&bm.stats.HashedBytes, int64(len(data)))
	return blockID
}

func cloneBytes(b []byte) []byte {
	return append([]byte{}, b...)
}

// GetBlock gets the contents of a given block. If the block is not found returns blob.ErrBlobNotFound.
func (bm *Manager) GetBlock(ctx context.Context, blockID string) ([]byte, error) {
	bi, err := bm.getBlockInfo(blockID)
	if err != nil {
		return nil, err
	}

	if bi.Deleted {
		return nil, ErrBlockNotFound
	}

	return bm.getBlockContentsUnlocked(ctx, bi)
}

func (bm *Manager) getBlockInfo(blockID string) (Info, error) {
	bm.lock()
	defer bm.unlock()

	// check added blocks, not written to any packs.
	if bi, ok := bm.currentPackItems[blockID]; ok {
		return bi, nil
	}

	// added blocks, written to packs but not yet added to indexes
	if bi, ok := bm.packIndexBuilder[blockID]; ok {
		return *bi, nil
	}

	// read from committed block index
	return bm.committedBlocks.getBlock(blockID)
}

// BlockInfo returns information about a single block.
func (bm *Manager) BlockInfo(ctx context.Context, blockID string) (Info, error) {
	bi, err := bm.getBlockInfo(blockID)
	if err != nil {
		log.Debugf("BlockInfo(%q) - error %v", err)
		return Info{}, err
	}

	if bi.Deleted {
		log.Debugf("BlockInfo(%q) - deleted", blockID)
	} else {
		log.Debugf("BlockInfo(%q) - exists in %v", blockID, bi.PackBlobID)
	}

	return bi, err
}

// FindUnreferencedBlobs returns the list of unreferenced storage blocks.
func (bm *Manager) FindUnreferencedBlobs(ctx context.Context) ([]blob.Metadata, error) {
	infos, err := bm.ListBlockInfos("", true)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list index blocks")
	}

	usedPackBlocks := findPackBlocksInUse(infos)

	var unused []blob.Metadata
	err = bm.st.ListBlobs(ctx, PackBlobIDPrefix, func(bi blob.Metadata) error {
		u := usedPackBlocks[bi.BlobID]
		if u > 0 {
			log.Debugf("pack %v, in use by %v blocks", bi.BlobID, u)
			return nil
		}

		unused = append(unused, bi)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error listing storage blocks")
	}

	return unused, nil
}

func findPackBlocksInUse(infos []Info) map[blob.ID]int {
	packUsage := map[blob.ID]int{}

	for _, bi := range infos {
		packUsage[bi.PackBlobID]++
	}

	return packUsage
}

func (bm *Manager) getBlockContentsUnlocked(ctx context.Context, bi Info) ([]byte, error) {
	if bi.Payload != nil {
		return cloneBytes(bi.Payload), nil
	}

	payload, err := bm.blockCache.getContentBlock(ctx, blob.ID(bi.BlockID), bi.PackBlobID, int64(bi.PackOffset), int64(bi.Length))
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadBlocks, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	iv, err := getPackedBlockIV(bi.BlockID)
	if err != nil {
		return nil, err
	}

	decrypted, err := bm.decryptAndVerify(payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid checksum at %v offset %v length %v", bi.PackBlobID, bi.PackOffset, len(payload))
	}

	return decrypted, nil
}

func (bm *Manager) decryptAndVerify(encrypted []byte, iv []byte) ([]byte, error) {
	decrypted, err := bm.encryptor.Decrypt(encrypted, iv)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt")
	}

	atomic.AddInt64(&bm.stats.DecryptedBytes, int64(len(decrypted)))

	if bm.encryptor.IsAuthenticated() {
		// already verified
		return decrypted, nil
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	return decrypted, bm.verifyChecksum(decrypted, iv)
}

func (bm *Manager) getPhysicalBlockInternal(ctx context.Context, blobID blob.ID) ([]byte, error) {
	payload, err := bm.blockCache.getContentBlock(ctx, blobID, blobID, 0, -1)
	if err != nil {
		return nil, err
	}

	iv, err := getPhysicalBlockIV(blobID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadBlocks, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	payload, err = bm.encryptor.Decrypt(payload, iv)
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

func getPackedBlockIV(blockID string) ([]byte, error) {
	return hex.DecodeString(blockID[len(blockID)-(aes.BlockSize*2):])
}

func getPhysicalBlockIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 {
		s = s[0:p]
	}
	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):]))
}

func (bm *Manager) verifyChecksum(data []byte, blockID []byte) error {
	expected := bm.hasher(data)
	expected = expected[len(expected)-aes.BlockSize:]
	if !bytes.HasSuffix(blockID, expected) {
		atomic.AddInt32(&bm.stats.InvalidBlocks, 1)
		return errors.Errorf("invalid checksum for blob %x, expected %x", blockID, expected)
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

// Refresh reloads the committed block indexes.
func (bm *Manager) Refresh(ctx context.Context) (bool, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	log.Debugf("Refresh started")
	t0 := time.Now()
	_, updated, err := bm.loadPackIndexesUnlocked(ctx)
	log.Debugf("Refresh completed in %v and updated=%v", time.Since(t0), updated)
	return updated, err
}

type cachedList struct {
	Timestamp time.Time   `json:"timestamp"`
	Blocks    []IndexInfo `json:"blocks"`
}

// listIndexBlocksFromStorage returns the list of index blocks in the given storage.
// The list of blocks is not guaranteed to be sorted.
func listIndexBlocksFromStorage(ctx context.Context, st blob.Storage) ([]IndexInfo, error) {
	snapshot, err := blob.ListAllBlobsConsistent(ctx, st, newIndexBlockPrefix, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	var results []IndexInfo
	for _, it := range snapshot {
		ii := IndexInfo{
			BlobID:    it.BlobID,
			Timestamp: it.Timestamp,
			Length:    it.Length,
		}
		results = append(results, ii)
	}

	return results, err
}

// NewManager creates new block manager with given packing options and a formatter.
func NewManager(ctx context.Context, st blob.Storage, f FormattingOptions, caching CachingOptions, repositoryFormatBytes []byte) (*Manager, error) {
	return newManagerWithOptions(ctx, st, f, caching, time.Now, repositoryFormatBytes)
}

func newManagerWithOptions(ctx context.Context, st blob.Storage, f FormattingOptions, caching CachingOptions, timeNow func() time.Time, repositoryFormatBytes []byte) (*Manager, error) {
	if f.Version < minSupportedReadVersion || f.Version > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", f.Version, minSupportedReadVersion, maxSupportedReadVersion)
	}

	hasher, encryptor, err := CreateHashAndEncryptor(f)
	if err != nil {
		return nil, err
	}

	blockCache, err := newBlockCache(ctx, st, caching)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize block cache")
	}

	listCache, err := newListCache(ctx, st, caching)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize list cache")
	}

	blockIndex, err := newCommittedBlockIndex(caching)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize committed block index")
	}

	m := &Manager{
		Format:                f,
		timeNow:               timeNow,
		flushPackIndexesAfter: timeNow().Add(flushPackIndexTimeout),
		maxPackSize:           f.MaxPackSize,
		encryptor:             encryptor,
		hasher:                hasher,
		currentPackItems:      make(map[string]Info),
		packIndexBuilder:      make(packIndexBuilder),
		committedBlocks:       blockIndex,
		minPreambleLength:     defaultMinPreambleLength,
		maxPreambleLength:     defaultMaxPreambleLength,
		paddingUnit:           defaultPaddingUnit,
		blockCache:            blockCache,
		listCache:             listCache,
		st:                    st,
		repositoryFormatBytes: repositoryFormatBytes,

		writeFormatVersion:      int32(f.Version),
		closed:                  make(chan struct{}),
		checkInvariantsOnUnlock: os.Getenv("KOPIA_VERIFY_INVARIANTS") != "",
	}

	m.startPackIndexLocked()

	if err := m.CompactIndexes(ctx, autoCompactionOptions); err != nil {
		return nil, errors.Wrap(err, "error initializing block manager")
	}

	return m, nil
}

func CreateHashAndEncryptor(f FormattingOptions) (HashFunc, Encryptor, error) {
	h, err := createHashFunc(f)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := createEncryptor(f)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create encryptor")
	}

	blockID := h(nil)
	_, err = e.Encrypt(nil, blockID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "invalid encryptor")
	}

	return h, e, nil
}

func createHashFunc(f FormattingOptions) (HashFunc, error) {
	h := hashFunctions[f.Hash]
	if h == nil {
		return nil, errors.Errorf("unknown hash function %v", f.Hash)
	}

	hashFunc, err := h(f)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize hash")
	}

	if hashFunc == nil {
		return nil, errors.Errorf("nil hash function returned for %v", f.Hash)
	}

	return hashFunc, nil
}

func createEncryptor(f FormattingOptions) (Encryptor, error) {
	e := encryptors[f.Encryption]
	if e == nil {
		return nil, errors.Errorf("unknown encryption algorithm: %v", f.Encryption)
	}

	return e(f)
}
