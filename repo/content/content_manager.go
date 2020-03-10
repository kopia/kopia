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

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var (
	log       = logging.GetContextLoggerFunc("kopia/content")
	formatLog = logging.GetContextLoggerFunc("kopia/content/format")
)

// Prefixes for pack blobs
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
	mu       *sync.RWMutex
	cond     *sync.Cond
	flushing bool

	pendingPacks     map[blob.ID]*pendingPackInfo
	writingPacks     []*pendingPackInfo // list of packs that are being written
	failedPacks      []*pendingPackInfo // list of packs that failed to write, will be retried
	packIndexBuilder packIndexBuilder   // contents that are in index currently being built (all packs saved but not committed)

	disableIndexFlushCount int
	flushPackIndexesAfter  time.Time // time when those indexes should be flushed
	closed                 chan struct{}

	lockFreeManager
}

type pendingPackInfo struct {
	prefix                blob.ID
	currentPackItems      map[ID]Info // contents that are in the pack content currently being built (all inline)
	currentPackDataLength int         // total length of all items in the current pack content
}

// DeleteContent marks the given contentID as deleted.
//
// NOTE: To avoid race conditions only contents that cannot be possibly re-created
// should ever be deleted. That means that contents of such contents should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *Manager) DeleteContent(ctx context.Context, contentID ID) error {
	bm.lock()
	defer bm.unlock()

	log(ctx).Debugf("DeleteContent(%q)", contentID)

	// remove from all pending packs
	for _, pp := range bm.pendingPacks {
		if bi, ok := pp.currentPackItems[contentID]; ok && !bi.Deleted {
			delete(pp.currentPackItems, contentID)
			return nil
		}
	}

	// remove from all packs that are being written, since they will be committed to index soon
	for _, pp := range bm.writingPacks {
		if bi, ok := pp.currentPackItems[contentID]; ok && !bi.Deleted {
			bm.deletePreexistingContent(bi)
			return nil
		}
	}

	// if found in committed index, add another entry that's marked for deletion
	if bi, ok := bm.packIndexBuilder[contentID]; ok {
		bm.deletePreexistingContent(*bi)
		return nil
	}

	// see if the block existed before
	bi, err := bm.committedContents.getContent(contentID)
	if err != nil {
		return err
	}

	bm.deletePreexistingContent(bi)

	return nil
}

// Intentionally passing bi by value.
// nolint:gocritic
func (bm *Manager) deletePreexistingContent(ci Info) {
	if ci.Deleted {
		return
	}

	pp := bm.getOrCreatePendingPackInfoLocked(packPrefixForContentID(ci.ID))
	ci.Deleted = true
	ci.TimestampSeconds = bm.timeNow().Unix()
	pp.currentPackItems[ci.ID] = ci
}

func (bm *Manager) addToPackUnlocked(ctx context.Context, contentID ID, data []byte, isDeleted bool) error {
	prefix := packPrefixForContentID(contentID)

	data = cloneBytes(data)

	bm.lock()

	// do not start new uploads while flushing
	for bm.flushing {
		formatLog(ctx).Debugf("waiting before flush completes")
		bm.cond.Wait()
	}

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		if err := bm.writePackAndAddToIndex(ctx, pp, true); err != nil {
			bm.unlock()
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

	if bm.timeNow().After(bm.flushPackIndexesAfter) {
		if err := bm.flushPackIndexesLocked(ctx); err != nil {
			bm.unlock()
			return err
		}
	}

	pp := bm.getOrCreatePendingPackInfoLocked(prefix)
	pp.currentPackDataLength += len(data)
	pp.currentPackItems[contentID] = Info{
		Deleted:          isDeleted,
		ID:               contentID,
		Payload:          data,
		Length:           uint32(len(data)),
		TimestampSeconds: bm.timeNow().Unix(),
	}

	shouldWrite := pp.currentPackDataLength >= bm.maxPackSize
	if shouldWrite {
		// we're about to write to storage without holding a lock
		// remove from pendingPacks so other goroutine tries to mess with this pending pack.
		delete(bm.pendingPacks, pp.prefix)
		bm.writingPacks = append(bm.writingPacks, pp)
	}

	bm.unlock()

	// at this point we're unlocked so different goroutines can encrypt and
	// save to storage in parallel.
	if shouldWrite {
		if err := bm.writePackAndAddToIndex(ctx, pp, false); err != nil {
			return errors.Wrap(err, "unable to write pack")
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
func (bm *Manager) DisableIndexFlush(ctx context.Context) {
	bm.lock()
	defer bm.unlock()
	log(ctx).Debugf("DisableIndexFlush()")
	bm.disableIndexFlushCount++
}

// EnableIndexFlush decrements the counter preventing automatic index flushes.
// The flushes will be reenabled when the index drops to zero.
func (bm *Manager) EnableIndexFlush(ctx context.Context) {
	bm.lock()
	defer bm.unlock()
	log(ctx).Debugf("EnableIndexFlush()")
	bm.disableIndexFlushCount--
}

func (bm *Manager) verifyInvariantsLocked() {
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
	if bm.disableIndexFlushCount > 0 {
		log(ctx).Debugf("not flushing index because flushes are currently disabled")
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

		if err := bm.committedContents.addContent(ctx, indexBlobID, dataCopy, true); err != nil {
			return errors.Wrap(err, "unable to add committed content")
		}

		bm.packIndexBuilder = make(packIndexBuilder)
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)

	return nil
}

func (bm *Manager) finishAllPacksLocked(ctx context.Context) error {
	for prefix, pp := range bm.pendingPacks {
		delete(bm.pendingPacks, prefix)
		bm.writingPacks = append(bm.writingPacks, pp)

		if err := bm.writePackAndAddToIndex(ctx, pp, true); err != nil {
			return errors.Wrap(err, "error writing pack content")
		}
	}

	return nil
}

func (bm *Manager) writePackAndAddToIndex(ctx context.Context, pp *pendingPackInfo, holdingLock bool) error {
	packFileIndex, err := bm.prepareAndWritePackInternal(ctx, pp)

	if !holdingLock {
		bm.lock()

		defer func() {
			bm.cond.Broadcast()
			bm.unlock()
		}()
	}

	// after finishing writing, remove from both writingPacks and failedPacks
	bm.writingPacks = removePendingPack(bm.writingPacks, pp)
	bm.failedPacks = removePendingPack(bm.failedPacks, pp)

	if err == nil {
		// success, add pack index builder entries to index.
		for _, info := range packFileIndex {
			bm.packIndexBuilder.Add(*info)
		}

		return nil
	}

	// failure - add to failedPacks slice again
	bm.failedPacks = append(bm.failedPacks, pp)

	return errors.Wrap(err, "error writing pack")
}

func (bm *Manager) prepareAndWritePackInternal(ctx context.Context, pp *pendingPackInfo) (packIndexBuilder, error) {
	contentID := make([]byte, 16)
	if _, err := cryptorand.Read(contentID); err != nil {
		return nil, errors.Wrap(err, "unable to read crypto bytes")
	}

	packFile := blob.ID(fmt.Sprintf("%v%x", pp.prefix, contentID))

	contentData, packFileIndex, err := bm.preparePackDataContent(ctx, pp, packFile)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing data content")
	}

	if len(contentData) > 0 {
		if err := bm.writePackFileNotLocked(ctx, packFile, contentData); err != nil {
			return nil, errors.Wrap(err, "can't save pack data content")
		}

		formatLog(ctx).Debugf("wrote pack file: %v (%v bytes)", packFile, len(contentData))
	}

	return packFileIndex, nil
}

func removePendingPack(slice []*pendingPackInfo, pp *pendingPackInfo) []*pendingPackInfo {
	result := slice[:0]

	for _, p := range slice {
		if p != pp {
			result = append(result, p)
		}
	}

	return result
}

// Close closes the content manager.
func (bm *Manager) Close(ctx context.Context) error {
	if err := bm.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing")
	}

	bm.contentCache.close()
	bm.metadataCache.close()
	close(bm.closed)

	return nil
}

// Flush completes writing any pending packs and writes pack indexes to the underlying storage.
// Any pending writes completed before Flush() has started are guaranteed to be committed to the
// repository before Flush() returns.
func (bm *Manager) Flush(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	bm.flushing = true

	defer func() {
		bm.flushing = false
	}()

	for len(bm.writingPacks) > 0 {
		log(ctx).Debugf("waiting for %v in-progress packs to finish", len(bm.writingPacks))

		// wait packs that are currently writing in other goroutines to finish
		bm.cond.Wait()
	}

	// finish all new pending packs
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

	return bm.addToPackUnlocked(ctx, contentID, data, bi.Deleted)
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
			prefix:           prefix,
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

	err := bm.addToPackUnlocked(ctx, contentID, data, false)

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

func (bm *Manager) getOverlayContentInfo(contentID ID) (Info, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	// check added contents, not written to any packs yet.
	for _, pp := range bm.pendingPacks {
		if ci, ok := pp.currentPackItems[contentID]; ok {
			return ci, true
		}
	}

	// check contents being written to packs right now.
	for _, pp := range bm.writingPacks {
		if ci, ok := pp.currentPackItems[contentID]; ok {
			return ci, true
		}
	}

	// added contents, written to packs but not yet added to indexes
	if ci, ok := bm.packIndexBuilder[contentID]; ok {
		return *ci, true
	}

	return Info{}, false
}

func (bm *Manager) getContentInfo(contentID ID) (Info, error) {
	if ci, ok := bm.getOverlayContentInfo(contentID); ok {
		return ci, nil
	}

	return bm.committedContents.getContent(contentID)
}

// ContentInfo returns information about a single content.
func (bm *Manager) ContentInfo(ctx context.Context, contentID ID) (Info, error) {
	bi, err := bm.getContentInfo(contentID)
	if err != nil {
		log(ctx).Debugf("ContentInfo(%q) - error %v", err)
		return Info{}, err
	}

	return bi, err
}

func (bm *Manager) lock() {
	bm.mu.Lock()
}

func (bm *Manager) unlock() {
	if bm.checkInvariantsOnUnlock {
		bm.verifyInvariantsLocked()
	}

	bm.mu.Unlock()
}

// Refresh reloads the committed content indexes.
func (bm *Manager) Refresh(ctx context.Context) (bool, error) {
	bm.lock()
	defer bm.unlock()

	log(ctx).Debugf("Refresh started")

	t0 := time.Now() // allow:no-inject-time

	_, updated, err := bm.loadPackIndexesUnlocked(ctx)
	log(ctx).Debugf("Refresh completed in %v and updated=%v", time.Since(t0), updated) // allow:no-inject-time

	return updated, err
}

// ManagerOptions are the optional parameters for manager creation
type ManagerOptions struct {
	RepositoryFormatBytes []byte
	TimeNow               func() time.Time // Time provider
}

// NewManager creates new content manager with given packing options and a formatter.
func NewManager(ctx context.Context, st blob.Storage, f *FormattingOptions, caching CachingOptions, options ManagerOptions) (*Manager, error) {
	nowFn := options.TimeNow
	if nowFn == nil {
		nowFn = time.Now // allow:no-inject-time
	}

	return newManagerWithOptions(ctx, st, f, caching, nowFn, options.RepositoryFormatBytes)
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

	mu := &sync.RWMutex{}
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

		mu:   mu,
		cond: sync.NewCond(mu),

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
