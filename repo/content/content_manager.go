// Package content implements repository support content-addressable storage contents.
package content

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
	log       = repologging.Logger("kopia/content")
	formatLog = repologging.Logger("kopia/content/format")
)

// PackBlobIDPrefix is the prefix for all pack blobs.
const PackBlobIDPrefix = "p"

const (
	parallelFetches          = 5                // number of parallel reads goroutines
	flushPackIndexTimeout    = 10 * time.Minute // time after which all pending indexes are flushes
	newIndexBlobPrefix       = "n"
	defaultMinPreambleLength = 32
	defaultMaxPreambleLength = 32
	defaultPaddingUnit       = 4096

	currentWriteVersion = 1

	minSupportedWriteVersion = 1
	maxSupportedWriteVersion = currentWriteVersion

	minSupportedReadVersion = 1
	maxSupportedReadVersion = currentWriteVersion

	indexLoadAttempts = 10
)

// ErrContentNotFound is returned when content is not found.
var ErrContentNotFound = errors.New("content not found")

// IndexBlobInfo is an information about a single index blob managed by Manager.
type IndexBlobInfo struct {
	BlobID    blob.ID
	Length    int64
	Timestamp time.Time
}

// Manager builds content-addressable storage with encryption, deduplication and packaging on top of BLOB store.
type Manager struct {
	Format FormattingOptions

	stats        Stats
	contentCache *contentCache
	listCache    *listCache
	st           blob.Storage

	mu                      sync.Mutex
	locked                  bool
	checkInvariantsOnUnlock bool

	currentPackItems      map[ID]Info      // contents that are in the pack content currently being built (all inline)
	currentPackDataLength int              // total length of all items in the current pack content
	packIndexBuilder      packIndexBuilder // contents that are in index currently being built (current pack and all packs saved but not committed)
	committedContents     *committedContentIndex

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

// DeleteContent marks the given contentID as deleted.
//
// NOTE: To avoid race conditions only contents that cannot be possibly re-created
// should ever be deleted. That means that contents of such contents should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *Manager) DeleteContent(contentID ID) error {
	bm.lock()
	defer bm.unlock()

	log.Debugf("DeleteContent(%q)", contentID)

	// We have this content in current pack index and it's already deleted there.
	if bi, ok := bm.packIndexBuilder[contentID]; ok {
		if !bi.Deleted {
			if bi.PackBlobID == "" {
				// added and never committed, just forget about it.
				delete(bm.packIndexBuilder, contentID)
				delete(bm.currentPackItems, contentID)
				return nil
			}

			// added and committed.
			bi2 := *bi
			bi2.Deleted = true
			bi2.TimestampSeconds = bm.timeNow().Unix()
			bm.setPendingContent(bi2)
		}
		return nil
	}

	// We have this content in current pack index and it's already deleted there.
	bi, err := bm.committedContents.getContent(contentID)
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
	bm.setPendingContent(bi2)
	return nil
}

//nolint:gocritic
// We're intentionally passing "i" by value
func (bm *Manager) setPendingContent(i Info) {
	bm.packIndexBuilder.Add(i)
	bm.currentPackItems[i.ID] = i
}

func (bm *Manager) addToPackLocked(ctx context.Context, contentID ID, data []byte, isDeleted bool) error {
	bm.assertLocked()

	data = cloneBytes(data)
	bm.currentPackDataLength += len(data)
	bm.setPendingContent(Info{
		Deleted:          isDeleted,
		ID:               contentID,
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

// Stats returns statistics about content manager operations.
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
		bm.assertInvariant(cpi.ID == k, "content ID entry has invalid key: %v %v", cpi.ID, k)
		bm.assertInvariant(cpi.Deleted || cpi.PackBlobID == "", "content ID entry has unexpected pack content ID %v: %v", cpi.ID, cpi.PackBlobID)
		bm.assertInvariant(cpi.TimestampSeconds != 0, "content has no timestamp: %v", cpi.ID)
		bi, ok := bm.packIndexBuilder[k]
		bm.assertInvariant(ok, "content ID entry not present in pack index builder: %v", cpi.ID)
		bm.assertInvariant(reflect.DeepEqual(*bi, cpi), "current pack index does not match pack index builder: %v", cpi, *bi)
	}
}

func (bm *Manager) verifyPackIndexBuilderLocked() {
	for k, cpi := range bm.packIndexBuilder {
		bm.assertInvariant(cpi.ID == k, "content ID entry has invalid key: %v %v", cpi.ID, k)
		if _, ok := bm.currentPackItems[cpi.ID]; ok {
			// ignore contents also in currentPackItems
			continue
		}
		if cpi.Deleted {
			bm.assertInvariant(cpi.PackBlobID == "", "content can't be both deleted and have a pack content: %v", cpi.ID)
		} else {
			bm.assertInvariant(cpi.PackBlobID != "", "content that's not deleted must have a pack content: %+v", cpi)
			bm.assertInvariant(cpi.FormatVersion == byte(bm.writeFormatVersion), "content that's not deleted must have a valid format version: %+v", cpi)
		}
		bm.assertInvariant(cpi.TimestampSeconds != 0, "content has no timestamp: %v", cpi.ID)
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
	bm.currentPackItems = make(map[ID]Info)
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

		if err := bm.committedContents.addContent(indexBlobID, dataCopy, true); err != nil {
			return errors.Wrap(err, "unable to add committed content")
		}
		bm.packIndexBuilder = make(packIndexBuilder)
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)
	return nil
}

func (bm *Manager) writePackIndexesNew(ctx context.Context, data []byte) (blob.ID, error) {
	return bm.encryptAndWriteContentNotLocked(ctx, data, newIndexBlobPrefix)
}

func (bm *Manager) finishPackLocked(ctx context.Context) error {
	if len(bm.currentPackItems) == 0 {
		log.Debugf("no current pack entries")
		return nil
	}

	if err := bm.writePackContentLocked(ctx); err != nil {
		return errors.Wrap(err, "error writing pack content")
	}

	bm.startPackIndexLocked()
	return nil
}

func (bm *Manager) writePackContentLocked(ctx context.Context) error {
	bm.assertLocked()

	contentID := make([]byte, 16)
	if _, err := cryptorand.Read(contentID); err != nil {
		return errors.Wrap(err, "unable to read crypto bytes")
	}

	packFile := blob.ID(fmt.Sprintf("%v%x", PackBlobIDPrefix, contentID))

	contentData, packFileIndex, err := bm.preparePackDataContent(packFile)
	if err != nil {
		return errors.Wrap(err, "error preparing data content")
	}

	if len(contentData) > 0 {
		if err := bm.writePackFileNotLocked(ctx, packFile, contentData); err != nil {
			return errors.Wrap(err, "can't save pack data content")
		}
	}

	formatLog.Debugf("wrote pack file: %v (%v bytes)", packFile, len(contentData))
	for _, info := range packFileIndex {
		bm.packIndexBuilder.Add(*info)
	}

	return nil
}

func (bm *Manager) preparePackDataContent(packFile blob.ID) ([]byte, packIndexBuilder, error) {
	formatLog.Debugf("preparing content data with %v items", len(bm.currentPackItems))

	contentData, err := appendRandomBytes(append([]byte(nil), bm.repositoryFormatBytes...), rand.Intn(bm.maxPreambleLength-bm.minPreambleLength+1)+bm.minPreambleLength)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to prepare content preamble")
	}

	packFileIndex := packIndexBuilder{}
	for contentID, info := range bm.currentPackItems {
		if info.Payload == nil {
			continue
		}

		var encrypted []byte
		encrypted, err = bm.maybeEncryptContentDataForPacking(info.Payload, info.ID)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unable to encrypt %q", contentID)
		}

		formatLog.Debugf("adding %v length=%v deleted=%v", contentID, len(info.Payload), info.Deleted)

		packFileIndex.Add(Info{
			ID:               contentID,
			Deleted:          info.Deleted,
			FormatVersion:    byte(bm.writeFormatVersion),
			PackBlobID:       packFile,
			PackOffset:       uint32(len(contentData)),
			Length:           uint32(len(encrypted)),
			TimestampSeconds: info.TimestampSeconds,
		})

		contentData = append(contentData, encrypted...)
	}

	if len(packFileIndex) == 0 {
		return nil, nil, nil
	}

	if bm.paddingUnit > 0 {
		if missing := bm.paddingUnit - (len(contentData) % bm.paddingUnit); missing > 0 {
			contentData, err = appendRandomBytes(contentData, missing)
			if err != nil {
				return nil, nil, errors.Wrap(err, "unable to prepare content postamble")
			}
		}
	}

	origContentLength := len(contentData)
	contentData, err = bm.appendPackFileIndexRecoveryData(contentData, packFileIndex)

	formatLog.Debugf("finished content %v bytes (%v bytes index)", len(contentData), len(contentData)-origContentLength)
	return contentData, packFileIndex, err
}

func (bm *Manager) maybeEncryptContentDataForPacking(data []byte, contentID ID) ([]byte, error) {
	iv, err := getPackedContentIV(contentID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get packed content IV for %q", contentID)
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

// IndexBlobs returns the list of active index blobs.
func (bm *Manager) IndexBlobs(ctx context.Context) ([]IndexBlobInfo, error) {
	return bm.listCache.listIndexBlobs(ctx)
}

func (bm *Manager) loadPackIndexesUnlocked(ctx context.Context) ([]IndexBlobInfo, bool, error) {
	nextSleepTime := 100 * time.Millisecond

	for i := 0; i < indexLoadAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		if i > 0 {
			bm.listCache.deleteListCache()
			log.Debugf("encountered NOT_FOUND when loading, sleeping %v before retrying #%v", nextSleepTime, i)
			time.Sleep(nextSleepTime)
			nextSleepTime *= 2
		}

		contents, err := bm.listCache.listIndexBlobs(ctx)
		if err != nil {
			return nil, false, err
		}

		err = bm.tryLoadPackIndexBlobsUnlocked(ctx, contents)
		if err == nil {
			var contentIDs []blob.ID
			for _, b := range contents {
				contentIDs = append(contentIDs, b.BlobID)
			}
			var updated bool
			updated, err = bm.committedContents.use(contentIDs)
			if err != nil {
				return nil, false, err
			}
			return contents, updated, nil
		}
		if err != blob.ErrBlobNotFound {
			return nil, false, err
		}
	}

	return nil, false, errors.Errorf("unable to load pack indexes despite %v retries", indexLoadAttempts)
}

func (bm *Manager) tryLoadPackIndexBlobsUnlocked(ctx context.Context, contents []IndexBlobInfo) error {
	ch, unprocessedIndexesSize, err := bm.unprocessedIndexBlobsUnlocked(contents)
	if err != nil {
		return err
	}
	if len(ch) == 0 {
		return nil
	}

	log.Infof("downloading %v new index blobs (%v bytes)...", len(ch), unprocessedIndexesSize)
	var wg sync.WaitGroup

	errch := make(chan error, parallelFetches)

	for i := 0; i < parallelFetches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for indexBlobID := range ch {
				data, err := bm.getIndexBlobInternal(ctx, indexBlobID)
				if err != nil {
					errch <- err
					return
				}

				if err := bm.committedContents.addContent(indexBlobID, data, false); err != nil {
					errch <- errors.Wrap(err, "unable to add to committed content cache")
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
	log.Infof("Index contents downloaded.")

	return nil
}

// unprocessedIndexBlobsUnlocked returns a closed channel filled with content IDs that are not in committedContents cache.
func (bm *Manager) unprocessedIndexBlobsUnlocked(contents []IndexBlobInfo) (resultCh <-chan blob.ID, totalSize int64, err error) {
	ch := make(chan blob.ID, len(contents))
	for _, c := range contents {
		has, err := bm.committedContents.cache.hasIndexBlobID(c.BlobID)
		if err != nil {
			return nil, 0, err
		}
		if has {
			log.Debugf("index blob %q already in cache, skipping", c.BlobID)
			continue
		}
		ch <- c.BlobID
		totalSize += c.Length
	}
	close(ch)
	return ch, totalSize, nil
}

// Close closes the content manager.
func (bm *Manager) Close() {
	bm.contentCache.close()
	close(bm.closed)
}

// ListContents returns IDs of contents matching given prefix.
func (bm *Manager) ListContents(prefix ID) ([]ID, error) {
	bm.lock()
	defer bm.unlock()

	var result []ID

	appendToResult := func(i Info) error {
		if i.Deleted || !strings.HasPrefix(string(i.ID), string(prefix)) {
			return nil
		}
		if bi, ok := bm.packIndexBuilder[i.ID]; ok && bi.Deleted {
			return nil
		}
		result = append(result, i.ID)
		return nil
	}

	for _, bi := range bm.packIndexBuilder {
		_ = appendToResult(*bi)
	}

	_ = bm.committedContents.listContents(prefix, appendToResult)
	return result, nil
}

// ListContentInfos returns the metadata about contents with a given prefix and kind.
func (bm *Manager) ListContentInfos(prefix ID, includeDeleted bool) ([]Info, error) {
	bm.lock()
	defer bm.unlock()

	var result []Info

	appendToResult := func(i Info) error {
		if (i.Deleted && !includeDeleted) || !strings.HasPrefix(string(i.ID), string(prefix)) {
			return nil
		}
		if bi, ok := bm.packIndexBuilder[i.ID]; ok && bi.Deleted {
			return nil
		}
		result = append(result, i)
		return nil
	}

	for _, bi := range bm.packIndexBuilder {
		_ = appendToResult(*bi)
	}

	_ = bm.committedContents.listContents(prefix, appendToResult)

	return result, nil
}

// Flush completes writing any pending packs and writes pack indexes to the underlyign storage.
func (bm *Manager) Flush(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	if err := bm.finishPackLocked(ctx); err != nil {
		return errors.Wrap(err, "error writing pending content")
	}

	if err := bm.flushPackIndexesLocked(ctx); err != nil {
		return errors.Wrap(err, "error flushing indexes")
	}

	return nil
}

// RewriteContent causes reads and re-writes a given content using the most recent format.
func (bm *Manager) RewriteContent(ctx context.Context, contentID ID) error {
	bi, err := bm.getContentInfo(contentID)
	if err != nil {
		return err
	}

	data, err := bm.getContentDataUnlocked(ctx, &bi)
	if err != nil {
		return err
	}

	bm.lock()
	defer bm.unlock()
	return bm.addToPackLocked(ctx, contentID, data, bi.Deleted)
}

// WriteContent saves a given content of data to a pack group with a provided name and returns a contentID
// that's based on the contents of data written.
func (bm *Manager) WriteContent(ctx context.Context, data []byte, prefix ID) (ID, error) {
	if err := validatePrefix(prefix); err != nil {
		return "", err
	}
	contentID := prefix + ID(hex.EncodeToString(bm.hashData(data)))

	// content already tracked
	if bi, err := bm.getContentInfo(contentID); err == nil {
		if !bi.Deleted {
			return contentID, nil
		}
	}

	log.Debugf("WriteContent(%q) - new", contentID)
	bm.lock()
	defer bm.unlock()
	err := bm.addToPackLocked(ctx, contentID, data, false)
	return contentID, err
}

func validatePrefix(prefix ID) error {
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
	atomic.AddInt32(&bm.stats.WrittenContents, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data)))
	bm.listCache.deleteListCache()
	return bm.st.PutBlob(ctx, packFile, data)
}

func (bm *Manager) encryptAndWriteContentNotLocked(ctx context.Context, data []byte, prefix blob.ID) (blob.ID, error) {
	hash := bm.hashData(data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	// Encrypt the content in-place.
	atomic.AddInt64(&bm.stats.EncryptedBytes, int64(len(data)))
	data2, err := bm.encryptor.Encrypt(data, hash)
	if err != nil {
		return "", err
	}

	atomic.AddInt32(&bm.stats.WrittenContents, 1)
	atomic.AddInt64(&bm.stats.WrittenBytes, int64(len(data2)))
	bm.listCache.deleteListCache()
	if err := bm.st.PutBlob(ctx, blobID, data2); err != nil {
		return "", err
	}

	return blobID, nil
}

func (bm *Manager) hashData(data []byte) []byte {
	// Hash the content and compute encryption key.
	contentID := bm.hasher(data)
	atomic.AddInt32(&bm.stats.HashedContents, 1)
	atomic.AddInt64(&bm.stats.HashedBytes, int64(len(data)))
	return contentID
}

func cloneBytes(b []byte) []byte {
	return append([]byte{}, b...)
}

// GetContent gets the contents of a given content. If the content is not found returns ErrContentNotFound.
func (bm *Manager) GetContent(ctx context.Context, contentID ID) ([]byte, error) {
	bi, err := bm.getContentInfo(contentID)
	if err != nil {
		return nil, err
	}

	if bi.Deleted {
		return nil, ErrContentNotFound
	}

	return bm.getContentDataUnlocked(ctx, &bi)
}

func (bm *Manager) getContentInfo(contentID ID) (Info, error) {
	bm.lock()
	defer bm.unlock()

	// check added contents, not written to any packs.
	if bi, ok := bm.currentPackItems[contentID]; ok {
		return bi, nil
	}

	// added contents, written to packs but not yet added to indexes
	if bi, ok := bm.packIndexBuilder[contentID]; ok {
		return *bi, nil
	}

	// read from committed content index
	return bm.committedContents.getContent(contentID)
}

// ContentInfo returns information about a single content.
func (bm *Manager) ContentInfo(ctx context.Context, contentID ID) (Info, error) {
	bi, err := bm.getContentInfo(contentID)
	if err != nil {
		log.Debugf("ContentInfo(%q) - error %v", err)
		return Info{}, err
	}

	if bi.Deleted {
		log.Debugf("ContentInfo(%q) - deleted", contentID)
	} else {
		log.Debugf("ContentInfo(%q) - exists in %v", contentID, bi.PackBlobID)
	}

	return bi, err
}

// FindUnreferencedBlobs returns the list of unreferenced storage contents.
func (bm *Manager) FindUnreferencedBlobs(ctx context.Context) ([]blob.Metadata, error) {
	infos, err := bm.ListContentInfos("", true)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list index blobs")
	}

	usedPackContents := findPackContentsInUse(infos)

	var unused []blob.Metadata
	err = bm.st.ListBlobs(ctx, PackBlobIDPrefix, func(bi blob.Metadata) error {
		u := usedPackContents[bi.BlobID]
		if u > 0 {
			log.Debugf("pack %v, in use by %v contents", bi.BlobID, u)
			return nil
		}

		unused = append(unused, bi)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error listing storage contents")
	}

	return unused, nil
}

func findPackContentsInUse(infos []Info) map[blob.ID]int {
	packUsage := map[blob.ID]int{}

	for _, bi := range infos {
		packUsage[bi.PackBlobID]++
	}

	return packUsage
}

func (bm *Manager) getContentDataUnlocked(ctx context.Context, bi *Info) ([]byte, error) {
	if bi.Payload != nil {
		return cloneBytes(bi.Payload), nil
	}

	payload, err := bm.contentCache.getContentContent(ctx, blob.ID(bi.ID), bi.PackBlobID, int64(bi.PackOffset), int64(bi.Length))
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadContents, 1)
	atomic.AddInt64(&bm.stats.ReadBytes, int64(len(payload)))

	iv, err := getPackedContentIV(bi.ID)
	if err != nil {
		return nil, err
	}

	decrypted, err := bm.decryptAndVerify(payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid checksum at %v offset %v length %v", bi.PackBlobID, bi.PackOffset, len(payload))
	}

	return decrypted, nil
}

func (bm *Manager) decryptAndVerify(encrypted, iv []byte) ([]byte, error) {
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

func (bm *Manager) getIndexBlobInternal(ctx context.Context, blobID blob.ID) ([]byte, error) {
	payload, err := bm.contentCache.getContentContent(ctx, blobID, blobID, 0, -1)
	if err != nil {
		return nil, err
	}

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt32(&bm.stats.ReadContents, 1)
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

func getPackedContentIV(contentID ID) ([]byte, error) {
	return hex.DecodeString(string(contentID[len(contentID)-(aes.BlockSize*2):]))
}

func getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { // nolint:gocritic
		s = s[0:p]
	}
	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):]))
}

func (bm *Manager) verifyChecksum(data, contentID []byte) error {
	expected := bm.hasher(data)
	expected = expected[len(expected)-aes.BlockSize:]
	if !bytes.HasSuffix(contentID, expected) {
		atomic.AddInt32(&bm.stats.InvalidContents, 1)
		return errors.Errorf("invalid checksum for blob %x, expected %x", contentID, expected)
	}

	atomic.AddInt32(&bm.stats.ValidContents, 1)
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

// Refresh reloads the committed content indexes.
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
	Timestamp time.Time       `json:"timestamp"`
	Contents  []IndexBlobInfo `json:"contents"`
}

// listIndexBlobsFromStorage returns the list of index blobs in the given storage.
// The list of contents is not guaranteed to be sorted.
func listIndexBlobsFromStorage(ctx context.Context, st blob.Storage) ([]IndexBlobInfo, error) {
	snapshot, err := blob.ListAllBlobsConsistent(ctx, st, newIndexBlobPrefix, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	var results []IndexBlobInfo
	for _, it := range snapshot {
		ii := IndexBlobInfo{
			BlobID:    it.BlobID,
			Timestamp: it.Timestamp,
			Length:    it.Length,
		}
		results = append(results, ii)
	}

	return results, err
}

// NewManager creates new content manager with given packing options and a formatter.
func NewManager(ctx context.Context, st blob.Storage, f *FormattingOptions, caching CachingOptions, repositoryFormatBytes []byte) (*Manager, error) {
	return newManagerWithOptions(ctx, st, f, caching, time.Now, repositoryFormatBytes)
}

func newManagerWithOptions(ctx context.Context, st blob.Storage, f *FormattingOptions, caching CachingOptions, timeNow func() time.Time, repositoryFormatBytes []byte) (*Manager, error) {
	if f.Version < minSupportedReadVersion || f.Version > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", f.Version, minSupportedReadVersion, maxSupportedReadVersion)
	}

	if f.Version < minSupportedWriteVersion || f.Version > currentWriteVersion {
		return nil, errors.Errorf("can't handle repositories created using version %v (min supported %v, max supported %v)", f.Version, minSupportedWriteVersion, maxSupportedWriteVersion)
	}

	hasher, encryptor, err := CreateHashAndEncryptor(f)
	if err != nil {
		return nil, err
	}

	contentCache, err := newContentCache(ctx, st, caching)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize content cache")
	}

	listCache, err := newListCache(st, caching)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize list cache")
	}

	contentIndex := newCommittedContentIndex(caching)

	m := &Manager{
		Format:                *f,
		timeNow:               timeNow,
		flushPackIndexesAfter: timeNow().Add(flushPackIndexTimeout),
		maxPackSize:           f.MaxPackSize,
		encryptor:             encryptor,
		hasher:                hasher,
		currentPackItems:      make(map[ID]Info),
		packIndexBuilder:      make(packIndexBuilder),
		committedContents:     contentIndex,
		minPreambleLength:     defaultMinPreambleLength,
		maxPreambleLength:     defaultMaxPreambleLength,
		paddingUnit:           defaultPaddingUnit,
		contentCache:          contentCache,
		listCache:             listCache,
		st:                    st,
		repositoryFormatBytes: repositoryFormatBytes,

		writeFormatVersion:      int32(f.Version),
		closed:                  make(chan struct{}),
		checkInvariantsOnUnlock: os.Getenv("KOPIA_VERIFY_INVARIANTS") != "",
	}

	m.startPackIndexLocked()

	if err := m.CompactIndexes(ctx, autoCompactionOptions); err != nil {
		return nil, errors.Wrap(err, "error initializing content manager")
	}

	return m, nil
}

func CreateHashAndEncryptor(f *FormattingOptions) (HashFunc, Encryptor, error) {
	h, err := createHashFunc(f)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create hash")
	}

	e, err := createEncryptor(f)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create encryptor")
	}

	contentID := h(nil)
	_, err = e.Encrypt(nil, contentID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "invalid encryptor")
	}

	return h, e, nil
}

func createHashFunc(f *FormattingOptions) (HashFunc, error) {
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

func createEncryptor(f *FormattingOptions) (Encryptor, error) {
	e := encryptors[f.Encryption]
	if e == nil {
		return nil, errors.Errorf("unknown encryption algorithm: %v", f.Encryption)
	}

	return e(f)
}
