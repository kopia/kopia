// Package content implements repository support for content-addressable storage.
package content

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var (
	log       = logging.GetContextLoggerFunc("kopia/content")
	formatLog = logging.GetContextLoggerFunc(FormatLogModule)
)

// Prefixes for pack blobs.
const (
	PackBlobIDPrefixRegular blob.ID = "p"
	PackBlobIDPrefixSpecial blob.ID = "q"

	FormatLogModule = "kopia/format"

	maxHashSize                            = 64
	defaultEncryptionBufferPoolSegmentSize = 8 << 20 // 8 MB
)

// PackBlobIDPrefixes contains all possible prefixes for pack blobs.
var PackBlobIDPrefixes = []blob.ID{
	PackBlobIDPrefixRegular,
	PackBlobIDPrefixSpecial,
}

const (
	parallelFetches          = 5                // number of parallel reads goroutines
	flushPackIndexTimeout    = 10 * time.Minute // time after which all pending indexes are flushes
	indexBlobPrefix          = "n"
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
	blob.Metadata
	Superseded []blob.Metadata
}

// WriteManager builds content-addressable storage with encryption, deduplication and packaging on top of BLOB store.
type WriteManager struct {
	revision int64 // changes on each local write

	mu       *sync.RWMutex
	cond     *sync.Cond
	flushing bool

	sessionUser string // user@host to report as session owners
	sessionHost string

	currentSessionInfo   SessionInfo
	sessionMarkerBlobIDs []blob.ID // session marker blobs written so far

	pendingPacks     map[blob.ID]*pendingPackInfo
	writingPacks     []*pendingPackInfo // list of packs that are being written
	failedPacks      []*pendingPackInfo // list of packs that failed to write, will be retried
	packIndexBuilder packIndexBuilder   // contents that are in index currently being built (all packs saved but not committed)

	disableIndexFlushCount int
	flushPackIndexesAfter  time.Time // time when those indexes should be flushed

	onUpload func(int64)

	*SharedManager
}

type pendingPackInfo struct {
	prefix           blob.ID
	packBlobID       blob.ID
	currentPackItems map[ID]Info         // contents that are in the pack content currently being built (all inline)
	currentPackData  *gather.WriteBuffer // total length of all items in the current pack content
	finalized        bool                // indicates whether currentPackData has local index appended to it
}

// Revision returns data revision number that changes on each write or refresh.
func (bm *WriteManager) Revision() int64 {
	return atomic.LoadInt64(&bm.revision) + bm.committedContents.revision()
}

// DeleteContent marks the given contentID as deleted.
//
// NOTE: To avoid race conditions only contents that cannot be possibly re-created
// should ever be deleted. That means that contents of such contents should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *WriteManager) DeleteContent(ctx context.Context, contentID ID) error {
	bm.lock()
	defer bm.unlock()

	atomic.AddInt64(&bm.revision, 1)

	formatLog(ctx).Debugf("delete-content %v", contentID)

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
			return bm.deletePreexistingContent(ctx, bi)
		}
	}

	// if found in committed index, add another entry that's marked for deletion
	if bi, ok := bm.packIndexBuilder[contentID]; ok {
		return bm.deletePreexistingContent(ctx, *bi)
	}

	// see if the block existed before
	bi, err := bm.committedContents.getContent(contentID)
	if err != nil {
		return err
	}

	return bm.deletePreexistingContent(ctx, bi)
}

// Intentionally passing bi by value.
func (bm *WriteManager) deletePreexistingContent(ctx context.Context, ci Info) error {
	if ci.Deleted {
		return nil
	}

	pp, err := bm.getOrCreatePendingPackInfoLocked(ctx, packPrefixForContentID(ci.ID))
	if err != nil {
		return errors.Wrap(err, "unable to create pack")
	}

	ci.Deleted = true
	ci.TimestampSeconds = bm.timeNow().Unix()
	pp.currentPackItems[ci.ID] = ci

	return nil
}

func (bm *WriteManager) maybeFlushBasedOnTimeUnlocked(ctx context.Context) error {
	bm.lock()
	shouldFlush := bm.timeNow().After(bm.flushPackIndexesAfter)
	bm.unlock()

	if !shouldFlush {
		return nil
	}

	return bm.Flush(ctx)
}

func (bm *WriteManager) maybeRetryWritingFailedPacksUnlocked(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	// do not start new uploads while flushing
	for bm.flushing {
		formatLog(ctx).Debugf("wait-before-retry")
		bm.cond.Wait()
	}

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		formatLog(ctx).Debugf("retry-write %v", pp.packBlobID)

		if err := bm.writePackAndAddToIndex(ctx, pp, true); err != nil {
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

	return nil
}

func (bm *WriteManager) addToPackUnlocked(ctx context.Context, contentID ID, data []byte, isDeleted bool) error {
	// see if the current index is old enough to cause automatic flush.
	if err := bm.maybeFlushBasedOnTimeUnlocked(ctx); err != nil {
		return errors.Wrap(err, "unable to flush old pending writes")
	}

	prefix := packPrefixForContentID(contentID)

	bm.lock()

	atomic.AddInt64(&bm.revision, 1)

	// do not start new uploads while flushing
	for bm.flushing {
		formatLog(ctx).Debugf("wait-before-flush")
		bm.cond.Wait()
	}

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		formatLog(ctx).Debugf("retry-write %v", pp.packBlobID)

		if err := bm.writePackAndAddToIndex(ctx, pp, true); err != nil {
			bm.unlock()
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

	pp, err := bm.getOrCreatePendingPackInfoLocked(ctx, prefix)
	if err != nil {
		bm.unlock()
		return errors.Wrap(err, "unable to create pending pack")
	}

	info := Info{
		Deleted:          isDeleted,
		ID:               contentID,
		PackBlobID:       pp.packBlobID,
		PackOffset:       uint32(pp.currentPackData.Length()),
		TimestampSeconds: bm.timeNow().Unix(),
		FormatVersion:    byte(bm.writeFormatVersion),
	}

	if err := bm.maybeEncryptContentDataForPacking(pp.currentPackData, data, contentID); err != nil {
		return errors.Wrapf(err, "unable to encrypt %q", contentID)
	}

	info.Length = uint32(pp.currentPackData.Length()) - info.PackOffset

	pp.currentPackItems[contentID] = info

	shouldWrite := pp.currentPackData.Length() >= bm.maxPackSize
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

// DisableIndexFlush increments the counter preventing automatic index flushes.
func (bm *WriteManager) DisableIndexFlush(ctx context.Context) {
	bm.lock()
	defer bm.unlock()
	log(ctx).Debugf("DisableIndexFlush()")
	bm.disableIndexFlushCount++
}

// EnableIndexFlush decrements the counter preventing automatic index flushes.
// The flushes will be reenabled when the index drops to zero.
func (bm *WriteManager) EnableIndexFlush(ctx context.Context) {
	bm.lock()
	defer bm.unlock()
	log(ctx).Debugf("EnableIndexFlush()")
	bm.disableIndexFlushCount--
}

func (bm *WriteManager) verifyInvariantsLocked() {
	bm.verifyCurrentPackItemsLocked()
	bm.verifyPackIndexBuilderLocked()
}

func (bm *WriteManager) verifyCurrentPackItemsLocked() {
	for _, pp := range bm.pendingPacks {
		for k, cpi := range pp.currentPackItems {
			bm.assertInvariant(cpi.ID == k, "content ID entry has invalid key: %v %v", cpi.ID, k)

			if !cpi.Deleted {
				bm.assertInvariant(cpi.PackBlobID == pp.packBlobID, "non-deleted pending pack item %q must be from the pending pack %q, was %q", cpi.ID, pp.packBlobID, cpi.PackBlobID)
			}

			bm.assertInvariant(cpi.TimestampSeconds != 0, "content has no timestamp: %v", cpi.ID)
		}
	}
}

func (bm *WriteManager) verifyPackIndexBuilderLocked() {
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

func (bm *WriteManager) assertInvariant(ok bool, errorMsg string, arg ...interface{}) {
	if ok {
		return
	}

	if len(arg) > 0 {
		errorMsg = fmt.Sprintf(errorMsg, arg...)
	}

	panic(errorMsg)
}

func (bm *WriteManager) flushPackIndexesLocked(ctx context.Context) error {
	if bm.disableIndexFlushCount > 0 {
		log(ctx).Debugf("not flushing index because flushes are currently disabled")
		return nil
	}

	if len(bm.packIndexBuilder) > 0 {
		var b bytes.Buffer

		if err := bm.packIndexBuilder.Build(&b); err != nil {
			return errors.Wrap(err, "unable to build pack index")
		}

		data := b.Bytes()
		dataCopy := append([]byte(nil), data...)

		bm.onUpload(int64(len(data)))

		indexBlobMD, err := bm.indexBlobManager.writeIndexBlob(ctx, data, bm.currentSessionInfo.ID)
		if err != nil {
			return errors.Wrap(err, "error writing index blob")
		}

		if err := bm.commitSession(ctx); err != nil {
			return errors.Wrap(err, "unable to commit session")
		}

		// if we managed to commit the session marker blobs, the index is now fully committed
		// and will be visible to others, including blob GC.
		if err := bm.committedContents.addContent(ctx, indexBlobMD.BlobID, dataCopy, true); err != nil {
			return errors.Wrap(err, "unable to add committed content")
		}

		bm.packIndexBuilder = make(packIndexBuilder)
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)

	return nil
}

func (bm *WriteManager) finishAllPacksLocked(ctx context.Context) error {
	for prefix, pp := range bm.pendingPacks {
		delete(bm.pendingPacks, prefix)
		bm.writingPacks = append(bm.writingPacks, pp)

		if err := bm.writePackAndAddToIndex(ctx, pp, true); err != nil {
			return errors.Wrap(err, "error writing pack content")
		}
	}

	return nil
}

func (bm *WriteManager) writePackAndAddToIndex(ctx context.Context, pp *pendingPackInfo, holdingLock bool) error {
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

		pp.currentPackData.Close()

		return nil
	}

	// failure - add to failedPacks slice again
	bm.failedPacks = append(bm.failedPacks, pp)

	return errors.Wrap(err, "error writing pack")
}

func (bm *WriteManager) prepareAndWritePackInternal(ctx context.Context, pp *pendingPackInfo) (packIndexBuilder, error) {
	packFileIndex, err := bm.preparePackDataContent(ctx, pp)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing data content")
	}

	if pp.currentPackData.Length() > 0 {
		if err := bm.writePackFileNotLocked(ctx, pp.packBlobID, pp.currentPackData.Bytes()); err != nil {
			formatLog(ctx).Debugf("failed-pack %v %v", pp.packBlobID, err)
			return nil, errors.Wrapf(err, "can't save pack data blob %v", pp.packBlobID)
		}

		formatLog(ctx).Debugf("wrote-pack %v %v", pp.packBlobID, pp.currentPackData.Length())
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

// ContentFormat returns formatting options.
func (bm *WriteManager) ContentFormat() FormattingOptions {
	return bm.format
}

// Close closes the content manager.
func (bm *WriteManager) Close(ctx context.Context) error {
	return bm.SharedManager.release(ctx)
}

// Flush completes writing any pending packs and writes pack indexes to the underlying storage.
// Any pending writes completed before Flush() has started are guaranteed to be committed to the
// repository before Flush() returns.
func (bm *WriteManager) Flush(ctx context.Context) error {
	bm.lock()
	defer bm.unlock()

	formatLog(ctx).Debugf("flush")

	bm.flushing = true

	defer func() {
		bm.flushing = false

		// we finished flushing, notify goroutines that were waiting for it.
		bm.cond.Broadcast()
	}()

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		formatLog(ctx).Debugf("retry-write %v", pp.packBlobID)

		if err := bm.writePackAndAddToIndex(ctx, pp, true); err != nil {
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

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
func (bm *WriteManager) RewriteContent(ctx context.Context, contentID ID) error {
	formatLog(ctx).Debugf("rewrite-content %v", contentID)

	pp, bi, err := bm.getContentInfo(contentID)
	if err != nil {
		return err
	}

	data, err := bm.getContentDataUnlocked(ctx, pp, &bi)
	if err != nil {
		return err
	}

	return bm.addToPackUnlocked(ctx, contentID, data, bi.Deleted)
}

// UndeleteContent rewrites the content with the given ID if the content exists
// and is mark deleted. If the content exists and is not marked deleted, this
// operation is a no-op.
func (bm *WriteManager) UndeleteContent(ctx context.Context, contentID ID) error {
	log(ctx).Debugf("UndeleteContent(%q)", contentID)

	pp, bi, err := bm.getContentInfo(contentID)
	if err != nil {
		return err
	}

	if !bi.Deleted {
		return nil
	}

	data, err := bm.getContentDataUnlocked(ctx, pp, &bi)
	if err != nil {
		return err
	}

	return bm.addToPackUnlocked(ctx, contentID, data, false)
}

func packPrefixForContentID(contentID ID) blob.ID {
	if contentID.HasPrefix() {
		return PackBlobIDPrefixSpecial
	}

	return PackBlobIDPrefixRegular
}

func (bm *WriteManager) getOrCreatePendingPackInfoLocked(ctx context.Context, prefix blob.ID) (*pendingPackInfo, error) {
	if pp := bm.pendingPacks[prefix]; pp != nil {
		return pp, nil
	}

	b := gather.NewWriteBuffer()

	sessionID, err := bm.getOrStartSessionLocked(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get session ID")
	}

	blobID := make([]byte, 16)
	if _, err := cryptorand.Read(blobID); err != nil {
		return nil, errors.Wrap(err, "unable to read crypto bytes")
	}

	b.Append(bm.repositoryFormatBytes)

	// nolint:gosec
	if err := writeRandomBytesToBuffer(b, rand.Intn(bm.maxPreambleLength-bm.minPreambleLength+1)+bm.minPreambleLength); err != nil {
		return nil, errors.Wrap(err, "unable to prepare content preamble")
	}

	bm.pendingPacks[prefix] = &pendingPackInfo{
		prefix:           prefix,
		packBlobID:       blob.ID(fmt.Sprintf("%v%x-%v", prefix, blobID, sessionID)),
		currentPackItems: map[ID]Info{},
		currentPackData:  b,
	}

	return bm.pendingPacks[prefix], nil
}

// WriteContent saves a given content of data to a pack group with a provided name and returns a contentID
// that's based on the contents of data written.
func (bm *WriteManager) WriteContent(ctx context.Context, data []byte, prefix ID) (ID, error) {
	if err := bm.maybeRetryWritingFailedPacksUnlocked(ctx); err != nil {
		return "", err
	}

	stats.Record(ctx, metricContentWriteContentCount.M(1))
	stats.Record(ctx, metricContentWriteContentBytes.M(int64(len(data))))

	if err := ValidatePrefix(prefix); err != nil {
		return "", err
	}

	var hashOutput [maxHashSize]byte

	contentID := prefix + ID(hex.EncodeToString(bm.hashData(hashOutput[:0], data)))

	// content already tracked
	if _, bi, err := bm.getContentInfo(contentID); err == nil {
		if !bi.Deleted {
			formatLog(ctx).Debugf("write-content %v already-exists", contentID)
			return contentID, nil
		}

		formatLog(ctx).Debugf("write-content %v previously-deleted", contentID)
	} else {
		formatLog(ctx).Debugf("write-content %v new", contentID)
	}

	err := bm.addToPackUnlocked(ctx, contentID, data, false)

	return contentID, err
}

// GetContent gets the contents of a given content. If the content is not found returns ErrContentNotFound.
func (bm *WriteManager) GetContent(ctx context.Context, contentID ID) (v []byte, err error) {
	defer func() {
		switch {
		case err == nil:
			stats.Record(ctx,
				metricContentGetCount.M(1),
				metricContentGetBytes.M(int64(len(v))))
		case errors.Is(err, ErrContentNotFound):
			stats.Record(ctx, metricContentGetNotFoundCount.M(1))
		default:
			stats.Record(ctx, metricContentGetErrorCount.M(1))
		}
	}()

	pp, bi, err := bm.getContentInfo(contentID)
	if err != nil {
		return nil, err
	}

	// Return content even if it is bi.Deleted so it can be recovered during GC among others.
	return bm.getContentDataUnlocked(ctx, pp, &bi)
}

func (bm *WriteManager) getOverlayContentInfo(contentID ID) (*pendingPackInfo, Info, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	// check added contents, not written to any packs yet.
	for _, pp := range bm.pendingPacks {
		if ci, ok := pp.currentPackItems[contentID]; ok {
			return pp, ci, true
		}
	}

	// check contents being written to packs right now.
	for _, pp := range bm.writingPacks {
		if ci, ok := pp.currentPackItems[contentID]; ok {
			return pp, ci, true
		}
	}

	// added contents, written to packs but not yet added to indexes
	if ci, ok := bm.packIndexBuilder[contentID]; ok {
		return nil, *ci, true
	}

	return nil, Info{}, false
}

func (bm *WriteManager) getContentInfo(contentID ID) (*pendingPackInfo, Info, error) {
	if pp, ci, ok := bm.getOverlayContentInfo(contentID); ok {
		return pp, ci, nil
	}

	info, err := bm.committedContents.getContent(contentID)

	return nil, info, err
}

// ContentInfo returns information about a single content.
func (bm *WriteManager) ContentInfo(ctx context.Context, contentID ID) (Info, error) {
	_, bi, err := bm.getContentInfo(contentID)
	if err != nil {
		log(ctx).Debugf("ContentInfo(%q) - error %v", err)
		return Info{}, err
	}

	return bi, err
}

func (bm *WriteManager) lock() {
	bm.mu.Lock()
}

func (bm *WriteManager) unlock() {
	if bm.checkInvariantsOnUnlock {
		bm.verifyInvariantsLocked()
	}

	bm.mu.Unlock()
}

// Refresh reloads the committed content indexes.
func (bm *WriteManager) Refresh(ctx context.Context) (bool, error) {
	bm.lock()
	defer bm.unlock()

	log(ctx).Debugf("Refresh started")

	t0 := clock.Now()

	_, updated, err := bm.loadPackIndexesUnlocked(ctx)
	log(ctx).Debugf("Refresh completed in %v and updated=%v", clock.Since(t0), updated)

	return updated, err
}

// SyncMetadataCache synchronizes metadata cache with metadata blobs in storage.
func (bm *WriteManager) SyncMetadataCache(ctx context.Context) error {
	if cm, ok := bm.metadataCache.(*contentCacheForMetadata); ok {
		return cm.sync(ctx)
	}

	log(ctx).Debugf("metadata cache not enabled")

	return nil
}

// DecryptBlob returns the contents of an encrypted blob that can be decrypted (n,m,l).
func (bm *WriteManager) DecryptBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
	return bm.indexBlobManager.getIndexBlob(ctx, blobID)
}

// ManagerOptions are the optional parameters for manager creation.
type ManagerOptions struct {
	RepositoryFormatBytes []byte
	TimeNow               func() time.Time // Time provider

	ownWritesCache ownWritesCache // test hook to allow overriding own-writes cache
}

// CloneOrDefault returns a clone of provided ManagerOptions or default empty struct if nil.
func (o *ManagerOptions) CloneOrDefault() *ManagerOptions {
	if o == nil {
		return &ManagerOptions{}
	}

	o2 := *o

	return &o2
}

// NewManager creates new content manager with given packing options and a formatter.
func NewManager(ctx context.Context, st blob.Storage, f *FormattingOptions, caching *CachingOptions, options *ManagerOptions) (*WriteManager, error) {
	options = options.CloneOrDefault()
	if options.TimeNow == nil {
		options.TimeNow = clock.Now
	}

	sharedManager, err := NewSharedManager(ctx, st, f, caching, options)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing read manager")
	}

	return NewWriteManager(sharedManager, SessionOptions{}), nil
}

// SessionOptions specifies session options.
type SessionOptions struct {
	SessionUser string
	SessionHost string
	OnUpload    func(int64)
}

// NewWriteManager returns a session write manager.
func NewWriteManager(sm *SharedManager, options SessionOptions) *WriteManager {
	mu := &sync.RWMutex{}

	sm.addRef()

	if options.OnUpload == nil {
		options.OnUpload = func(int64) {}
	}

	return &WriteManager{
		SharedManager: sm,

		mu:   mu,
		cond: sync.NewCond(mu),

		flushPackIndexesAfter: sm.timeNow().Add(flushPackIndexTimeout),
		pendingPacks:          map[blob.ID]*pendingPackInfo{},
		packIndexBuilder:      make(packIndexBuilder),
		sessionUser:           options.SessionUser,
		sessionHost:           options.SessionHost,
		onUpload:              options.OnUpload,
	}
}
