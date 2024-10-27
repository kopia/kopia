// Package content implements repository support for content-addressable storage.
package content

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

// Prefixes for pack blobs.
const (
	PackBlobIDPrefixRegular blob.ID = "p"
	PackBlobIDPrefixSpecial blob.ID = "q"

	NoCompression compression.HeaderID = 0

	FormatLogModule = "kopia/format"

	packBlobIDLength = 16

	DefaultIndexVersion = 2
)

var tracer = otel.Tracer("kopia/content")

// PackBlobIDPrefixes contains all possible prefixes for pack blobs.
//
//nolint:gochecknoglobals
var PackBlobIDPrefixes = []blob.ID{
	PackBlobIDPrefixRegular,
	PackBlobIDPrefixSpecial,
}

const (
	parallelFetches          = 5                // number of parallel reads goroutines
	flushPackIndexTimeout    = 10 * time.Minute // time after which all pending indexes are flushes
	defaultMinPreambleLength = 32
	defaultMaxPreambleLength = 32
	defaultPaddingUnit       = 4096

	indexLoadAttempts = 10
)

// ErrContentNotFound is returned when content is not found.
var ErrContentNotFound = errors.New("content not found")

// WriteManager builds content-addressable storage with encryption, deduplication and packaging on top of BLOB store.
type WriteManager struct {
	revision            atomic.Int64 // changes on each local write
	disableIndexRefresh atomic.Bool

	mu sync.RWMutex
	// +checklocks:mu
	cond *sync.Cond
	// +checklocks:mu
	flushing bool

	sessionUser string // user@host to report as session owners
	sessionHost string

	currentSessionInfo   SessionInfo
	sessionMarkerBlobIDs []blob.ID // session marker blobs written so far

	// +checklocks:mu
	pendingPacks map[blob.ID]*pendingPackInfo
	// +checklocks:mu
	writingPacks []*pendingPackInfo // list of packs that are being written
	// +checklocks:mu
	failedPacks []*pendingPackInfo // list of packs that failed to write, will be retried
	// +checklocks:mu
	packIndexBuilder index.Builder // contents that are in index currently being built (all packs saved but not committed)

	// +checklocks:mu
	disableIndexFlushCount int
	// +checklocks:mu
	flushPackIndexesAfter time.Time // time when those indexes should be flushed

	onUpload func(int64)

	*SharedManager

	log logging.Logger
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
	return bm.revision.Load() + bm.committedContents.revision()
}

// DeleteContent marks the given contentID as deleted.
//
// NOTE: To avoid race conditions only contents that cannot be possibly re-created
// should ever be deleted. That means that contents of such contents should include some element
// of randomness or a contemporaneous timestamp that will never reappear.
func (bm *WriteManager) DeleteContent(ctx context.Context, contentID ID) error {
	bm.lock()
	defer bm.unlock(ctx)

	bm.revision.Add(1)

	bm.log.Debugf("delete-content %v", contentID)

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
		return bm.deletePreexistingContent(ctx, bi)
	}

	// see if the content existed before
	if err := bm.maybeRefreshIndexes(ctx); err != nil {
		return err
	}

	bi, err := bm.committedContents.getContent(contentID)
	if err != nil {
		return err
	}

	return bm.deletePreexistingContent(ctx, bi)
}

func (bm *WriteManager) maybeRefreshIndexes(ctx context.Context) error {
	if bm.permissiveCacheLoading {
		return nil
	}

	if !bm.disableIndexRefresh.Load() && bm.shouldRefreshIndexes() {
		if err := bm.Refresh(ctx); err != nil {
			return errors.Wrap(err, "error refreshing indexes")
		}
	}

	return nil
}

// Intentionally passing bi by value.
// +checklocks:bm.mu
func (bm *WriteManager) deletePreexistingContent(ctx context.Context, ci Info) error {
	if ci.Deleted {
		return nil
	}

	pp, err := bm.getOrCreatePendingPackInfoLocked(ctx, packPrefixForContentID(ci.ContentID))
	if err != nil {
		return errors.Wrap(err, "unable to create pack")
	}

	pp.currentPackItems[ci.ContentID] = deletedInfo(ci, bm.contentWriteTime(ci.TimestampSeconds))

	return nil
}

func deletedInfo(is Info, deletedTime int64) Info {
	// clone and set deleted time
	is.Deleted = true
	is.TimestampSeconds = deletedTime

	return is
}

// contentWriteTime returns content write time for new content
// by computing max(timeNow().Unix(), previousUnixTimeSeconds + 1).
func (bm *WriteManager) contentWriteTime(previousUnixTimeSeconds int64) int64 {
	t := bm.timeNow().Unix()
	if t > previousUnixTimeSeconds {
		return t
	}

	return previousUnixTimeSeconds + 1
}

func (bm *WriteManager) maybeFlushBasedOnTimeUnlocked(ctx context.Context) error {
	bm.lock()
	shouldFlush := bm.timeNow().After(bm.flushPackIndexesAfter)
	bm.unlock(ctx)

	if !shouldFlush {
		return nil
	}

	return bm.Flush(ctx)
}

func (bm *WriteManager) maybeRetryWritingFailedPacksUnlocked(ctx context.Context) error {
	bm.lock()
	defer bm.unlock(ctx)

	// do not start new uploads while flushing
	for bm.flushing {
		bm.log.Debug("wait-before-retry")
		bm.cond.Wait()
	}

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		bm.log.Debugf("retry-write %v", pp.packBlobID)

		if err := bm.writePackAndAddToIndexLocked(ctx, pp); err != nil {
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

	return nil
}

func (bm *WriteManager) addToPackUnlocked(ctx context.Context, contentID ID, data gather.Bytes, isDeleted bool, comp compression.HeaderID, previousWriteTime int64, mp format.MutableParameters) error {
	// see if the current index is old enough to cause automatic flush.
	if err := bm.maybeFlushBasedOnTimeUnlocked(ctx); err != nil {
		return errors.Wrap(err, "unable to flush old pending writes")
	}

	prefix := packPrefixForContentID(contentID)

	var compressedAndEncrypted gather.WriteBuffer
	defer compressedAndEncrypted.Close()

	// encrypt and compress before taking lock
	actualComp, err := bm.maybeCompressAndEncryptDataForPacking(data, contentID, comp, &compressedAndEncrypted, mp)
	if err != nil {
		return errors.Wrapf(err, "unable to encrypt %q", contentID)
	}

	bm.lock()

	if previousWriteTime < 0 {
		if _, _, err = bm.getContentInfoReadLocked(ctx, contentID); err == nil {
			// we lost the race while compressing the content, the content now exists.
			bm.unlock(ctx)
			return nil
		}
	}

	bm.revision.Add(1)

	// do not start new uploads while flushing
	for bm.flushing {
		bm.log.Debug("wait-before-flush")
		bm.cond.Wait()
	}

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		bm.log.Debugf("retry-write %v", pp.packBlobID)

		if err = bm.writePackAndAddToIndexLocked(ctx, pp); err != nil {
			bm.unlock(ctx)
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

	pp, err := bm.getOrCreatePendingPackInfoLocked(ctx, prefix)
	if err != nil {
		bm.unlock(ctx)
		return errors.Wrap(err, "unable to create pending pack")
	}

	info := Info{
		Deleted:          isDeleted,
		ContentID:        contentID,
		PackBlobID:       pp.packBlobID,
		PackOffset:       uint32(pp.currentPackData.Length()), //nolint:gosec
		TimestampSeconds: bm.contentWriteTime(previousWriteTime),
		FormatVersion:    byte(mp.Version),
		OriginalLength:   uint32(data.Length()), //nolint:gosec
	}

	if _, err := compressedAndEncrypted.Bytes().WriteTo(pp.currentPackData); err != nil {
		bm.unlock(ctx)
		return errors.Wrapf(err, "unable to append %q to pack data", contentID)
	}

	info.CompressionHeaderID = actualComp
	info.PackedLength = uint32(pp.currentPackData.Length()) - info.PackOffset //nolint:gosec

	pp.currentPackItems[contentID] = info

	shouldWrite := pp.currentPackData.Length() >= mp.MaxPackSize
	if shouldWrite {
		// we're about to write to storage without holding a lock
		// remove from pendingPacks so other goroutine tries to mess with this pending pack.
		delete(bm.pendingPacks, pp.prefix)
		bm.writingPacks = append(bm.writingPacks, pp)
	}

	bm.unlock(ctx)

	// at this point we're unlocked so different goroutines can encrypt and
	// save to storage in parallel.
	if shouldWrite {
		if err := bm.writePackAndAddToIndexUnlocked(ctx, pp); err != nil {
			return errors.Wrap(err, "unable to write pack")
		}
	}

	return nil
}

// DisableIndexFlush increments the counter preventing automatic index flushes.
func (bm *WriteManager) DisableIndexFlush(ctx context.Context) {
	bm.lock()
	defer bm.unlock(ctx)

	bm.log.Debug("DisableIndexFlush()")

	bm.disableIndexFlushCount++
}

// EnableIndexFlush decrements the counter preventing automatic index flushes.
// The flushes will be re-enabled when the index drops to zero.
func (bm *WriteManager) EnableIndexFlush(ctx context.Context) {
	bm.lock()
	defer bm.unlock(ctx)

	bm.log.Debug("EnableIndexFlush()")

	bm.disableIndexFlushCount--
}

// +checklocks:bm.mu
func (bm *WriteManager) verifyInvariantsLocked(mp format.MutableParameters) {
	bm.verifyCurrentPackItemsLocked()
	bm.verifyPackIndexBuilderLocked(mp)
}

// +checklocks:bm.mu
func (bm *WriteManager) verifyCurrentPackItemsLocked() {
	for _, pp := range bm.pendingPacks {
		for k, cpi := range pp.currentPackItems {
			assertInvariant(cpi.ContentID == k, "content ID entry has invalid key: %v %v", cpi.ContentID, k)

			if !cpi.Deleted {
				assertInvariant(cpi.PackBlobID == pp.packBlobID, "non-deleted pending pack item %q must be from the pending pack %q, was %q", cpi.ContentID, pp.packBlobID, cpi.PackBlobID)
			}

			assertInvariant(cpi.TimestampSeconds != 0, "content has no timestamp: %v", cpi.ContentID)
		}
	}
}

// +checklocks:bm.mu
func (bm *WriteManager) verifyPackIndexBuilderLocked(mp format.MutableParameters) {
	for k, cpi := range bm.packIndexBuilder {
		assertInvariant(cpi.ContentID == k, "content ID entry has invalid key: %v %v", cpi.ContentID, k)

		if cpi.Deleted {
			assertInvariant(cpi.PackBlobID == "", "content can't be both deleted and have a pack content: %v", cpi.ContentID)
		} else {
			assertInvariant(cpi.PackBlobID != "", "content that's not deleted must have a pack content: %+v", cpi)
			assertInvariant(cpi.FormatVersion == byte(mp.Version), "content that's not deleted must have a valid format version: %+v", cpi)
		}

		assertInvariant(cpi.TimestampSeconds != 0, "content has no timestamp: %v", cpi.ContentID)
	}
}

func assertInvariant(ok bool, errorMsg string, arg ...interface{}) {
	if ok {
		return
	}

	if len(arg) > 0 {
		errorMsg = fmt.Sprintf(errorMsg, arg...)
	}

	panic(errorMsg)
}

// +checklocksread:bm.indexesLock
func (bm *WriteManager) writeIndexBlobs(ctx context.Context, dataShards []gather.Bytes, sessionID SessionID) ([]blob.Metadata, error) {
	ctx, span := tracer.Start(ctx, "WriteIndexBlobs")
	defer span.End()

	ibm, err := bm.indexBlobManager(ctx)
	if err != nil {
		return nil, err
	}

	//nolint:wrapcheck
	return ibm.WriteIndexBlobs(ctx, dataShards, blob.ID(sessionID))
}

// +checklocksread:bm.indexesLock
func (bm *WriteManager) addIndexBlob(ctx context.Context, indexBlobID blob.ID, data gather.Bytes, use bool) error {
	ctx, span := tracer.Start(ctx, "AddIndexBlob")
	defer span.End()

	return bm.committedContents.addIndexBlob(ctx, indexBlobID, data, use)
}

// +checklocks:bm.mu
func (bm *WriteManager) flushPackIndexesLocked(ctx context.Context, mp format.MutableParameters) error {
	ctx, span := tracer.Start(ctx, "FlushPackIndexes")
	defer span.End()

	if bm.disableIndexFlushCount > 0 {
		bm.log.Debug("not flushing index because flushes are currently disabled")
		return nil
	}

	if len(bm.packIndexBuilder) > 0 {
		_, span2 := tracer.Start(ctx, "BuildShards")
		dataShards, closeShards, err := bm.packIndexBuilder.BuildShards(mp.IndexVersion, true, indexblob.DefaultIndexShardSize)

		span2.End()

		if err != nil {
			return errors.Wrap(err, "unable to build pack index")
		}

		defer closeShards()

		// we must hold a lock between writing an index and adding index blob to committed contents index
		// otherwise it is possible for concurrent compaction or refresh to forget about the blob we have just
		// written
		bm.indexesLock.RLock()
		defer bm.indexesLock.RUnlock()

		indexBlobMDs, err := bm.writeIndexBlobs(ctx, dataShards, bm.currentSessionInfo.ID)
		if err != nil {
			return errors.Wrap(err, "error writing index blob")
		}

		if err := bm.commitSession(ctx); err != nil {
			return errors.Wrap(err, "unable to commit session")
		}

		// if we managed to commit the session marker blobs, the index is now fully committed
		// and will be visible to others, including blob GC.

		for i, indexBlobMD := range indexBlobMDs {
			bm.onUpload(int64(dataShards[i].Length()))

			if err := bm.addIndexBlob(ctx, indexBlobMD.BlobID, dataShards[i], true); err != nil {
				return errors.Wrap(err, "unable to add committed content")
			}
		}

		bm.packIndexBuilder = make(index.Builder)
	}

	bm.flushPackIndexesAfter = bm.timeNow().Add(flushPackIndexTimeout)

	return nil
}

// +checklocks:bm.mu
func (bm *WriteManager) finishAllPacksLocked(ctx context.Context) error {
	for prefix, pp := range bm.pendingPacks {
		delete(bm.pendingPacks, prefix)
		bm.writingPacks = append(bm.writingPacks, pp)

		if err := bm.writePackAndAddToIndexLocked(ctx, pp); err != nil {
			return errors.Wrap(err, "error writing pack content")
		}
	}

	return nil
}

func (bm *WriteManager) writePackAndAddToIndexUnlocked(ctx context.Context, pp *pendingPackInfo) error {
	// upload without lock
	packFileIndex, writeErr := bm.prepareAndWritePackInternal(ctx, pp, bm.onUpload)

	bm.lock()
	defer bm.unlock(ctx)

	return bm.processWritePackResultLocked(pp, packFileIndex, writeErr)
}

// +checklocks:bm.mu
func (bm *WriteManager) writePackAndAddToIndexLocked(ctx context.Context, pp *pendingPackInfo) error {
	packFileIndex, writeErr := bm.prepareAndWritePackInternal(ctx, pp, bm.onUpload)

	return bm.processWritePackResultLocked(pp, packFileIndex, writeErr)
}

// +checklocks:bm.mu
func (bm *WriteManager) processWritePackResultLocked(pp *pendingPackInfo, packFileIndex index.Builder, writeErr error) error {
	defer bm.cond.Broadcast()

	// after finishing writing, remove from both writingPacks and failedPacks
	bm.writingPacks = removePendingPack(bm.writingPacks, pp)
	bm.failedPacks = removePendingPack(bm.failedPacks, pp)

	if writeErr == nil {
		// success, add pack index builder entries to index.
		for _, info := range packFileIndex {
			bm.packIndexBuilder.Add(info)
		}

		pp.currentPackData.Close()

		return nil
	}

	// failure - add to failedPacks slice again
	bm.failedPacks = append(bm.failedPacks, pp)

	return errors.Wrap(writeErr, "error writing pack")
}

func (sm *SharedManager) prepareAndWritePackInternal(ctx context.Context, pp *pendingPackInfo, onUpload func(int64)) (index.Builder, error) {
	mp, mperr := sm.format.GetMutableParameters(ctx)
	if mperr != nil {
		return nil, errors.Wrap(mperr, "mutable parameters")
	}

	packFileIndex, err := sm.preparePackDataContent(mp, pp)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing data content")
	}

	if pp.currentPackData.Length() > 0 {
		if err := sm.writePackFileNotLocked(ctx, pp.packBlobID, pp.currentPackData.Bytes(), onUpload); err != nil {
			sm.log.Debugf("failed-pack %v %v", pp.packBlobID, err)
			return nil, errors.Wrapf(err, "can't save pack data blob %v", pp.packBlobID)
		}

		sm.log.Debugf("wrote-pack %v %v", pp.packBlobID, pp.currentPackData.Length())
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
func (bm *WriteManager) ContentFormat() format.Provider {
	return bm.format
}

// +checklocks:bm.mu
func (bm *WriteManager) setFlushingLocked(v bool) {
	bm.flushing = v
}

// Flush completes writing any pending packs and writes pack indexes to the underlying storage.
// Any pending writes completed before Flush() has started are guaranteed to be committed to the
// repository before Flush() returns.
func (bm *WriteManager) Flush(ctx context.Context) error {
	mp, mperr := bm.format.GetMutableParameters(ctx)
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	bm.lock()
	defer bm.unlock(ctx)

	bm.log.Debug("flush")

	// when finished flushing, notify goroutines that were waiting for it.
	defer bm.cond.Broadcast()

	bm.setFlushingLocked(true)
	defer bm.setFlushingLocked(false)

	// see if we have any packs that have failed previously
	// retry writing them now.
	//
	// we're making a copy of bm.failedPacks since bm.writePackAndAddToIndex()
	// will remove from it on success.
	fp := append([]*pendingPackInfo(nil), bm.failedPacks...)
	for _, pp := range fp {
		bm.log.Debugf("retry-write %v", pp.packBlobID)

		if err := bm.writePackAndAddToIndexLocked(ctx, pp); err != nil {
			return errors.Wrap(err, "error writing previously failed pack")
		}
	}

	for len(bm.writingPacks) > 0 {
		bm.log.Debugf("waiting for %v in-progress packs to finish", len(bm.writingPacks))

		// wait packs that are currently writing in other goroutines to finish
		bm.cond.Wait()
	}

	// finish all new pending packs
	if err := bm.finishAllPacksLocked(ctx); err != nil {
		return errors.Wrap(err, "error writing pending content")
	}

	if err := bm.flushPackIndexesLocked(ctx, mp); err != nil {
		return errors.Wrap(err, "error flushing indexes")
	}

	return nil
}

// RewriteContent causes reads and re-writes a given content using the most recent format.
// TODO(jkowalski): this will currently always re-encrypt and re-compress data, perhaps consider a
// pass-through mode that preserves encrypted/compressed bits.
func (bm *WriteManager) RewriteContent(ctx context.Context, contentID ID) error {
	bm.log.Debugf("rewrite-content %v", contentID)

	mp, mperr := bm.format.GetMutableParameters(ctx)
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	return bm.rewriteContent(ctx, contentID, false, mp)
}

func (bm *WriteManager) getContentDataAndInfo(ctx context.Context, contentID ID, output *gather.WriteBuffer) (Info, error) {
	// acquire read lock since to prevent flush from happening between getContentInfoReadLocked() and getContentDataReadLocked().
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	pp, bi, err := bm.getContentInfoReadLocked(ctx, contentID)
	if err != nil {
		return Info{}, err
	}

	if err := bm.getContentDataReadLocked(ctx, pp, bi, output); err != nil {
		return Info{}, err
	}

	return bi, nil
}

// UndeleteContent rewrites the content with the given ID if the content exists
// and is mark deleted. If the content exists and is not marked deleted, this
// operation is a no-op.
func (bm *WriteManager) UndeleteContent(ctx context.Context, contentID ID) error {
	bm.log.Debugf("UndeleteContent(%q)", contentID)

	mp, mperr := bm.format.GetMutableParameters(ctx)
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	return bm.rewriteContent(ctx, contentID, true, mp)
}

// When onlyRewriteDelete is true, the content is only rewritten if the existing
// content is marked as deleted. The new content is NOT marked deleted.
//
//	When onlyRewriteDelete is false, the content is unconditionally rewritten
//
// and the content's deleted status is preserved.
func (bm *WriteManager) rewriteContent(ctx context.Context, contentID ID, onlyRewriteDeleted bool, mp format.MutableParameters) error {
	var data gather.WriteBuffer
	defer data.Close()

	bi, err := bm.getContentDataAndInfo(ctx, contentID, &data)
	if err != nil {
		return errors.Wrap(err, "unable to get content data and info")
	}

	isDeleted := bi.Deleted

	if onlyRewriteDeleted {
		if !isDeleted {
			return nil
		}

		isDeleted = false
	}

	return bm.addToPackUnlocked(ctx, contentID, data.Bytes(), isDeleted, bi.CompressionHeaderID, bi.TimestampSeconds, mp)
}

func packPrefixForContentID(contentID ID) blob.ID {
	if contentID.HasPrefix() {
		return PackBlobIDPrefixSpecial
	}

	return PackBlobIDPrefixRegular
}

// +checklocks:bm.mu
func (bm *WriteManager) getOrCreatePendingPackInfoLocked(ctx context.Context, prefix blob.ID) (*pendingPackInfo, error) {
	if pp := bm.pendingPacks[prefix]; pp != nil {
		return pp, nil
	}

	bm.repoLogManager.Enable() // signal to the log manager that a write operation will be attempted so it is OK to write log blobs to the repo

	b := gather.NewWriteBuffer()

	sessionID, err := bm.getOrStartSessionLocked(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get session ID")
	}

	blobID := make([]byte, packBlobIDLength)
	if _, err := cryptorand.Read(blobID); err != nil {
		return nil, errors.Wrap(err, "unable to read crypto bytes")
	}

	suffix, berr := bm.format.RepositoryFormatBytes(ctx)
	if berr != nil {
		return nil, errors.Wrap(berr, "format bytes")
	}

	b.Append(suffix)

	//nolint:gosec
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

// SupportsContentCompression returns true if content manager supports content-compression.
func (bm *WriteManager) SupportsContentCompression() bool {
	mp := bm.format.GetCachedMutableParameters()

	return mp.IndexVersion >= index.Version2
}

// WriteContent saves a given content of data to a pack group with a provided name and returns a contentID
// that's based on the contents of data written.
func (bm *WriteManager) WriteContent(ctx context.Context, data gather.Bytes, prefix index.IDPrefix, comp compression.HeaderID) (ID, error) {
	t0 := timetrack.StartTimer()
	defer func() {
		bm.writeContentBytes.Observe(int64(data.Length()), t0.Elapsed())
	}()

	mp, mperr := bm.format.GetMutableParameters(ctx)
	if mperr != nil {
		return EmptyID, errors.Wrap(mperr, "mutable parameters")
	}

	if err := bm.maybeRetryWritingFailedPacksUnlocked(ctx); err != nil {
		return EmptyID, err
	}

	if err := prefix.ValidateSingle(); err != nil {
		return EmptyID, errors.Wrap(err, "invalid prefix")
	}

	var hashOutput [hashing.MaxHashSize]byte

	contentID, err := IDFromHash(prefix, bm.hashData(hashOutput[:0], data))
	if err != nil {
		return EmptyID, errors.Wrap(err, "invalid hash")
	}

	previousWriteTime := int64(-1)

	bm.mu.RLock()
	_, bi, err := bm.getContentInfoReadLocked(ctx, contentID)
	bm.mu.RUnlock()

	logbuf := logging.GetBuffer()
	defer logbuf.Release()

	logbuf.AppendString("write-content ")
	contentID.AppendToLogBuffer(logbuf)

	// content already tracked
	if err == nil {
		if !bi.Deleted {
			bm.deduplicatedContents.Add(1)
			bm.deduplicatedBytes.Add(int64(data.Length()))

			return contentID, nil
		}

		previousWriteTime = bi.TimestampSeconds

		logbuf.AppendString(" previously-deleted:")
		logbuf.AppendInt64(previousWriteTime)
	}

	bm.log.Debug(logbuf.String())

	return contentID, bm.addToPackUnlocked(ctx, contentID, data, false, comp, previousWriteTime, mp)
}

// GetContent gets the contents of a given content. If the content is not found returns ErrContentNotFound.
func (bm *WriteManager) GetContent(ctx context.Context, contentID ID) (v []byte, err error) {
	t0 := timetrack.StartTimer()

	defer func() {
		switch {
		case err == nil:
			bm.getContentBytes.Observe(int64(len(v)), t0.Elapsed())
		case errors.Is(err, ErrContentNotFound):
			bm.getContentNotFoundCount.Add(1)
		default:
			bm.getContentErrorCount.Add(1)
		}
	}()

	var tmp gather.WriteBuffer
	defer tmp.Close()

	_, err = bm.getContentDataAndInfo(ctx, contentID, &tmp)
	if err != nil {
		bm.log.Debugf("getContentInfoReadLocked(%v) error %v", contentID, err)
		return nil, err
	}

	return tmp.ToByteSlice(), nil
}

// +checklocksread:bm.mu
func (bm *WriteManager) getOverlayContentInfoReadLocked(contentID ID) (*pendingPackInfo, Info, bool) {
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
		return nil, ci, true
	}

	return nil, Info{}, false
}

// +checklocksread:bm.mu
func (bm *WriteManager) getContentInfoReadLocked(ctx context.Context, contentID ID) (*pendingPackInfo, Info, error) {
	if pp, ci, ok := bm.getOverlayContentInfoReadLocked(contentID); ok {
		return pp, ci, nil
	}

	// see if the content existed before
	if err := bm.maybeRefreshIndexes(ctx); err != nil {
		return nil, Info{}, err
	}

	info, err := bm.committedContents.getContent(contentID)

	return nil, info, err
}

// ContentInfo returns information about a single content.
func (bm *WriteManager) ContentInfo(ctx context.Context, contentID ID) (Info, error) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	_, bi, err := bm.getContentInfoReadLocked(ctx, contentID)
	if err != nil {
		bm.log.Debugf("ContentInfo(%q) - error %v", contentID, err)
		return Info{}, err
	}

	return bi, err
}

// DisableIndexRefresh disables index refresh for the remainder of this session.
func (bm *WriteManager) DisableIndexRefresh() {
	bm.disableIndexRefresh.Store(true)
}

// +checklocksacquire:bm.mu
func (bm *WriteManager) lock() {
	bm.mu.Lock()
}

// +checklocksrelease:bm.mu
func (bm *WriteManager) unlock(ctx context.Context) {
	if bm.checkInvariantsOnUnlock {
		mp, mperr := bm.format.GetMutableParameters(ctx)
		if mperr == nil {
			bm.verifyInvariantsLocked(mp)
		}
	}

	bm.mu.Unlock()
}

// MetadataCache returns an instance of metadata cache.
func (bm *WriteManager) MetadataCache() cache.ContentCache {
	return bm.metadataCache
}

// ManagerOptions are the optional parameters for manager creation.
type ManagerOptions struct {
	TimeNow                func() time.Time // Time provider
	DisableInternalLog     bool
	PermissiveCacheLoading bool
}

// CloneOrDefault returns a clone of provided ManagerOptions or default empty struct if nil.
func (o *ManagerOptions) CloneOrDefault() *ManagerOptions {
	if o == nil {
		return &ManagerOptions{}
	}

	o2 := *o

	return &o2
}

// NewManagerForTesting creates new content manager with given packing options and a formatter.
func NewManagerForTesting(ctx context.Context, st blob.Storage, f format.Provider, caching *CachingOptions, options *ManagerOptions) (*WriteManager, error) {
	options = options.CloneOrDefault()
	if options.TimeNow == nil {
		options.TimeNow = clock.Now
	}

	sharedManager, err := NewSharedManager(ctx, st, f, caching, options, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error initializing read manager")
	}

	return NewWriteManager(ctx, sharedManager, SessionOptions{}, ""), nil
}

// SessionOptions specifies session options.
type SessionOptions struct {
	SessionUser string
	SessionHost string
	OnUpload    func(int64)
}

// NewWriteManager returns a session write manager.
func NewWriteManager(ctx context.Context, sm *SharedManager, options SessionOptions, writeManagerID string) *WriteManager {
	if options.OnUpload == nil {
		options.OnUpload = func(int64) {}
	}

	wm := &WriteManager{
		SharedManager: sm,

		flushPackIndexesAfter: sm.timeNow().Add(flushPackIndexTimeout),
		pendingPacks:          map[blob.ID]*pendingPackInfo{},
		packIndexBuilder:      make(index.Builder),
		sessionUser:           options.SessionUser,
		sessionHost:           options.SessionHost,
		onUpload: func(numBytes int64) {
			options.OnUpload(numBytes)
			sm.uploadedBytes.Add(numBytes)
		},

		log: sm.namedLogger(writeManagerID),
	}

	wm.cond = sync.NewCond(&wm.mu)

	return wm
}
