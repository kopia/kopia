// Package content implements repository support for content-addressable storage.
package content

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/repo/blob"
)

var (
	log       = repologging.Logger("kopia/content")
	formatLog = repologging.Logger("kopia/content/format")
)

const (
	PackBlobIDPrefixRegular blob.ID = "p"
	PackBlobIDPrefixSpecial blob.ID = "q"
)

// PackBlobIDPrefixes contains all possible prefixes for pack blobs.
var PackBlobIDPrefixes = []blob.ID{
	PackBlobIDPrefixRegular,
	PackBlobIDPrefixSpecial,
}

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
	mu     sync.Mutex
	locked bool

	pendingPacks     map[blob.ID]*pendingPackInfo
	packIndexBuilder packIndexBuilder // contents that are in index currently being built (all packs saved but not committed)

	disableIndexFlushCount int
	flushPackIndexesAfter  time.Time // time when those indexes should be flushed
	closed                 chan struct{}

	lockFreeManager
}

type pendingPackInfo struct {
	currentPackItems      map[ID]Info // contents that are in the pack content currently being built (all inline)
	currentPackDataLength int         // total length of all items in the current pack content
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

	// remove from all pending packs
	for _, pp := range bm.pendingPacks {
		if bi, ok := pp.currentPackItems[contentID]; ok && !bi.Deleted {
			delete(pp.currentPackItems, contentID)
			return nil
		}
	}

	// if found in committed index, add another entry that's marked for deletion
	if bi, ok := bm.packIndexBuilder[contentID]; ok {
		if !bi.Deleted {
			// we have this content in index and it's not deleted.
			bi2 := *bi
			bi2.Deleted = true
			bi2.TimestampSeconds = bm.timeNow().Unix()
			bm.setPendingContent(bm.getOrCreatePendingPackInfoLocked(packPrefixForContentID(contentID)), bi2)
		}

		// we have this content in index and it already deleted - do nothing.
		return nil
	}

	// see if the block existed before
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

	bm.setPendingContent(bm.getOrCreatePendingPackInfoLocked(packPrefixForContentID(contentID)), bi2)
	return nil
}

//nolint:gocritic
// We're intentionally passing "i" by value
func (bm *Manager) setPendingContent(pp *pendingPackInfo, i Info) {
	pp.currentPackItems[i.ID] = i
}

func (bm *Manager) addToPackLocked(ctx context.Context, contentID ID, data []byte, isDeleted bool) error {
	bm.assertLocked()

	prefix := packPrefixForContentID(contentID)
	pp := bm.getOrCreatePendingPackInfoLocked(prefix)

	data = cloneBytes(data)
	pp.currentPackDataLength += len(data)
	bm.setPendingContent(pp, Info{
		Deleted:          isDeleted,
		ID:               contentID,
		Payload:          data,
		Length:           uint32(len(data)),
		TimestampSeconds: bm.timeNow().Unix(),
	})

	if pp.currentPackDataLength >= bm.maxPackSize {
		if err := bm.finishPackAndMaybeFlushIndexesLocked(ctx, prefix, pp); err != nil {
			return err
		}
	}

	return nil
}

func (bm *Manager) finishPackAndMaybeFlushIndexesLocked(ctx context.Context, prefix blob.ID, pp *pendingPackInfo) error {
	bm.assertLocked()

	if err := bm.finishPackLocked(ctx, prefix, pp); err != nil {
		return errors.Wrap(err, "unable to finish pack")
	}

	if bm.timeNow().After(bm.flushPackIndexesAfter) {
		if err := bm.finishAllPacksLocked(ctx); err != nil {
			return errors.Wrap(err, "finish all packs")
		}

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
	for _, pp := range bm.pendingPacks {
		for k, cpi := range pp.currentPackItems {
			bm.assertInvariant(cpi.ID == k, "content ID entry has invalid key: %v %v", cpi.ID, k)
			bm.assertInvariant(cpi.Deleted || cpi.PackBlobID == "", "content ID entry has unexpected pack content ID %v: %v", cpi.ID, cpi.PackBlobID)
			bm.assertInvariant(cpi.TimestampSeconds != 0, "content has no timestamp: %v", cpi.ID)
		}
	}
}

func (bm *Manager) verifyPackIndexBuilderLocked() {
	for k, cpi := range bm.packIndexBuilder {
		bm.assertInvariant(cpi.ID == k, "content ID entry has invalid key: %v %v", cpi.ID, k)
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

func (bm *Manager) finishAllPacksLocked(ctx context.Context) error {
	for prefix, pp := range bm.pendingPacks {
		if len(pp.currentPackItems) == 0 {
			log.Debugf("no current pack entries")
			continue
		}

		if err := bm.finishPackLocked(ctx, prefix, pp); err != nil {
			return errors.Wrap(err, "error writing pack content")
		}
	}

	return nil
}

func (bm *Manager) finishPackLocked(ctx context.Context, prefix blob.ID, pp *pendingPackInfo) error {
	bm.assertLocked()

	contentID := make([]byte, 16)
	if _, err := cryptorand.Read(contentID); err != nil {
		return errors.Wrap(err, "unable to read crypto bytes")
	}

	packFile := blob.ID(fmt.Sprintf("%v%x", prefix, contentID))
	contentData, packFileIndex, err := bm.preparePackDataContent(ctx, pp, packFile)
	if err != nil {
		return errors.Wrap(err, "error preparing data content")
	}

	if len(contentData) > 0 {
		if err := bm.writePackFileNotLocked(ctx, packFile, contentData); err != nil {
			return errors.Wrap(err, "can't save pack data content")
		}
		formatLog.Debugf("wrote pack file: %v (%v bytes)", packFile, len(contentData))
	}

	for _, info := range packFileIndex {
		bm.packIndexBuilder.Add(*info)
	}

	delete(bm.pendingPacks, prefix)
	return nil
}

// Close closes the content manager.
func (bm *Manager) Close() {
	bm.contentCache.close()
	bm.metadataCache.close()
	close(bm.closed)
}

// Flush completes writing any pending packs and writes pack indexes to the underlyign storage.
func (bm *Manager) Flush(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	if err := bm.finishAllPacksLocked(ctx); err != nil {
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

func packPrefixForContentID(contentID ID) blob.ID {
	if contentID.HasPrefix() {
		return PackBlobIDPrefixSpecial
	}
	return PackBlobIDPrefixRegular
}

func (bm *Manager) getOrCreatePendingPackInfoLocked(prefix blob.ID) *pendingPackInfo {
	if bm.pendingPacks[prefix] == nil {
		bm.pendingPacks[prefix] = &pendingPackInfo{
			currentPackItems: map[ID]Info{},
		}
	}

	return bm.pendingPacks[prefix]
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
	if bi, ok := bm.findContentInPendingPacks(contentID); ok {
		return bi, nil
	}

	// added contents, written to packs but not yet added to indexes
	if bi, ok := bm.packIndexBuilder[contentID]; ok {
		return *bi, nil
	}

	// read from committed content index
	return bm.committedContents.getContent(contentID)
}

func (bm *Manager) findContentInPendingPacks(contentID ID) (Info, bool) {
	for _, pp := range bm.pendingPacks {
		bi, ok := pp.currentPackItems[contentID]
		if ok {
			return bi, true
		}
	}

	return Info{}, false
}

// ContentInfo returns information about a single content.
func (bm *Manager) ContentInfo(ctx context.Context, contentID ID) (Info, error) {
	bi, err := bm.getContentInfo(contentID)
	if err != nil {
		log.Debugf("ContentInfo(%q) - error %v", err)
		return Info{}, err
	}

	return bi, err
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

	contentCache, err := newContentCache(ctx, st, caching, caching.MaxCacheSizeBytes, "contents")
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize content cache")
	}

	metadataCacheSize := caching.MaxMetadataCacheSizeBytes
	if metadataCacheSize == 0 && caching.MaxCacheSizeBytes > 0 {
		metadataCacheSize = caching.MaxCacheSizeBytes
	}

	metadataCache, err := newContentCache(ctx, st, caching, metadataCacheSize, "metadata")
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize metadata cache")
	}

	listCache, err := newListCache(st, caching)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize list cache")
	}

	contentIndex := newCommittedContentIndex(caching)

	m := &Manager{
		lockFreeManager: lockFreeManager{
			Format:                  *f,
			CachingOptions:          caching,
			timeNow:                 timeNow,
			maxPackSize:             f.MaxPackSize,
			encryptor:               encryptor,
			hasher:                  hasher,
			minPreambleLength:       defaultMinPreambleLength,
			maxPreambleLength:       defaultMaxPreambleLength,
			paddingUnit:             defaultPaddingUnit,
			contentCache:            contentCache,
			metadataCache:           metadataCache,
			listCache:               listCache,
			st:                      st,
			repositoryFormatBytes:   repositoryFormatBytes,
			checkInvariantsOnUnlock: os.Getenv("KOPIA_VERIFY_INVARIANTS") != "",
			writeFormatVersion:      int32(f.Version),
			committedContents:       contentIndex,
		},

		flushPackIndexesAfter: timeNow().Add(flushPackIndexTimeout),
		pendingPacks:          map[blob.ID]*pendingPackInfo{},
		packIndexBuilder:      make(packIndexBuilder),
		closed:                make(chan struct{}),
	}

	if err := m.CompactIndexes(ctx, autoCompactionOptions); err != nil {
		return nil, errors.Wrap(err, "error initializing content manager")
	}

	return m, nil
}
