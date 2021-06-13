package content

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

const (
	defaultEventualConsistencySettleTime = 1 * time.Hour
	compactionLogBlobPrefix              = "m"
	cleanupBlobPrefix                    = "l"
)

// compactionLogEntry represents contents of compaction log entry stored in `m` blob.
type compactionLogEntry struct {
	// list of input blob names that were compacted together.
	InputMetadata []blob.Metadata `json:"inputMetadata"`

	// list of blobs that are results of compaction.
	OutputMetadata []blob.Metadata `json:"outputMetadata"`

	// Metadata of the compaction blob itself, not serialized.
	metadata blob.Metadata
}

// cleanupEntry represents contents of cleanup entry stored in `l` blob.
type cleanupEntry struct {
	BlobIDs []blob.ID `json:"blobIDs"`

	// We're adding cleanup schedule time to make cleanup blobs unique which prevents them
	// from being rewritten, random would probably work just as well or another mechanism to prevent
	// deletion of blobs that does not require reading them in the first place (which messes up
	// read-after-create promise in S3).
	CleanupScheduleTime time.Time `json:"cleanupScheduleTime"`

	age time.Duration // not serialized, computed on load
}

type indexBlobManagerV0 struct {
	st           blob.Storage
	enc          *encryptedBlobMgr
	timeNow      func() time.Time
	log          logging.Logger
	maxPackSize  int
	indexVersion int
}

func (m *indexBlobManagerV0) listActiveIndexBlobs(ctx context.Context) ([]IndexBlobInfo, error) {
	return m.listIndexBlobs(ctx, false)
}

func (m *indexBlobManagerV0) listAllIndexBlobs(ctx context.Context) ([]IndexBlobInfo, error) {
	return m.listIndexBlobs(ctx, true)
}

func (m *indexBlobManagerV0) listIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	var compactionLogMetadata, storageIndexBlobs []blob.Metadata

	var eg errgroup.Group

	// list index and cleanup blobs in parallel.
	eg.Go(func() error {
		v, err := blob.ListAllBlobs(ctx, m.st, compactionLogBlobPrefix)
		compactionLogMetadata = v

		return errors.Wrap(err, "error listing compaction blobs")
	})

	eg.Go(func() error {
		v, err := blob.ListAllBlobs(ctx, m.st, IndexBlobPrefix)
		storageIndexBlobs = v

		return errors.Wrap(err, "error listing index blobs")
	})

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "error listing indexes")
	}

	for i, sib := range storageIndexBlobs {
		m.log.Debugf("found-index-blobs[%v] = %v", i, sib)
	}

	for i, clm := range compactionLogMetadata {
		m.log.Debugf("found-compaction-blobs[%v] %v", i, clm)
	}

	indexMap := map[blob.ID]*IndexBlobInfo{}
	addBlobsToIndex(indexMap, storageIndexBlobs)

	compactionLogs, err := m.getCompactionLogEntries(ctx, compactionLogMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error reading compaction log")
	}

	// remove entries from indexMap that have been compacted and replaced by other indexes.
	m.removeCompactedIndexes(indexMap, compactionLogs, includeInactive)

	var results []IndexBlobInfo
	for _, v := range indexMap {
		results = append(results, *v)
	}

	for i, res := range results {
		m.log.Debugf("active-index-blobs[%v] = %v", i, res)
	}

	return results, nil
}

func (m *indexBlobManagerV0) flushCache(ctx context.Context) {
	if err := m.st.FlushCaches(ctx); err != nil {
		m.log.Debugf("error flushing caches: %v", err)
	}
}

func (m *indexBlobManagerV0) compact(ctx context.Context, opt CompactOptions) error {
	indexBlobs, err := m.listActiveIndexBlobs(ctx)
	if err != nil {
		return errors.Wrap(err, "error listing active index blobs")
	}

	blobsToCompact := m.getBlobsToCompact(indexBlobs, opt)

	if err := m.compactIndexBlobs(ctx, blobsToCompact, opt); err != nil {
		return errors.Wrap(err, "error performing compaction")
	}

	if err := m.cleanup(ctx, opt.maxEventualConsistencySettleTime()); err != nil {
		return errors.Wrap(err, "error cleaning up index blobs")
	}

	return nil
}

func (m *indexBlobManagerV0) registerCompaction(ctx context.Context, inputs, outputs []blob.Metadata, maxEventualConsistencySettleTime time.Duration) error {
	logEntryBytes, err := json.Marshal(&compactionLogEntry{
		InputMetadata:  inputs,
		OutputMetadata: outputs,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobMetadata, err := m.enc.encryptAndWriteBlob(ctx, logEntryBytes, compactionLogBlobPrefix, "")
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	for i, input := range inputs {
		m.log.Debugf("compacted-input[%v/%v] %v", i, len(inputs), input)
	}

	for i, output := range outputs {
		m.log.Debugf("compacted-output[%v/%v] %v", i, len(outputs), output)
	}

	m.log.Debugf("compaction-log %v %v", compactionLogBlobMetadata.BlobID, compactionLogBlobMetadata.Timestamp)

	if err := m.deleteOldBlobs(ctx, compactionLogBlobMetadata, maxEventualConsistencySettleTime); err != nil {
		return errors.Wrap(err, "error deleting old index blobs")
	}

	return nil
}

func (m *indexBlobManagerV0) getIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
	return m.enc.getEncryptedBlob(ctx, blobID)
}

func (m *indexBlobManagerV0) writeIndexBlob(ctx context.Context, data []byte, sessionID SessionID) (blob.Metadata, error) {
	return m.enc.encryptAndWriteBlob(ctx, data, IndexBlobPrefix, sessionID)
}

func (m *indexBlobManagerV0) getCompactionLogEntries(ctx context.Context, blobs []blob.Metadata) (map[blob.ID]*compactionLogEntry, error) {
	results := map[blob.ID]*compactionLogEntry{}

	for _, cb := range blobs {
		data, err := m.enc.getEncryptedBlob(ctx, cb.BlobID)

		if errors.Is(err, blob.ErrBlobNotFound) {
			continue
		}

		if err != nil {
			return nil, errors.Wrapf(err, "unable to read compaction blob %q", cb.BlobID)
		}

		le := &compactionLogEntry{}

		if err := json.Unmarshal(data, le); err != nil {
			return nil, errors.Wrap(err, "unable to read compaction log entry %q")
		}

		le.metadata = cb

		results[cb.BlobID] = le
	}

	return results, nil
}

func (m *indexBlobManagerV0) getCleanupEntries(ctx context.Context, latestServerBlobTime time.Time, blobs []blob.Metadata) (map[blob.ID]*cleanupEntry, error) {
	results := map[blob.ID]*cleanupEntry{}

	for _, cb := range blobs {
		data, err := m.enc.getEncryptedBlob(ctx, cb.BlobID)

		if errors.Is(err, blob.ErrBlobNotFound) {
			continue
		}

		if err != nil {
			return nil, errors.Wrapf(err, "unable to read compaction blob %q", cb.BlobID)
		}

		le := &cleanupEntry{}

		if err := json.Unmarshal(data, le); err != nil {
			return nil, errors.Wrap(err, "unable to read compaction log entry %q")
		}

		le.age = latestServerBlobTime.Sub(le.CleanupScheduleTime)

		results[cb.BlobID] = le
	}

	return results, nil
}

func (m *indexBlobManagerV0) deleteOldBlobs(ctx context.Context, latestBlob blob.Metadata, maxEventualConsistencySettleTime time.Duration) error {
	allCompactionLogBlobs, err := blob.ListAllBlobs(ctx, m.st, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing compaction log blobs")
	}

	// look for server-assigned timestamp of the compaction log entry we just wrote as a reference.
	// we're assuming server-generated timestamps are somewhat reasonable and time is moving
	compactionLogServerTimeCutoff := latestBlob.Timestamp.Add(-maxEventualConsistencySettleTime)
	compactionBlobs := blobsOlderThan(allCompactionLogBlobs, compactionLogServerTimeCutoff)

	m.log.Debugf("fetching %v/%v compaction logs older than %v", len(compactionBlobs), len(allCompactionLogBlobs), compactionLogServerTimeCutoff)

	compactionBlobEntries, err := m.getCompactionLogEntries(ctx, compactionBlobs)
	if err != nil {
		return errors.Wrap(err, "unable to get compaction log entries")
	}

	indexBlobsToDelete := m.findIndexBlobsToDelete(latestBlob.Timestamp, compactionBlobEntries, maxEventualConsistencySettleTime)

	// note that we must always delete index blobs first before compaction logs
	// otherwise we may inadvertedly resurrect an index blob that should have been removed.
	if err := m.deleteBlobsFromStorageAndCache(ctx, indexBlobsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete compaction logs")
	}

	compactionLogBlobsToDelayCleanup := m.findCompactionLogBlobsToDelayCleanup(compactionBlobs)

	if err := m.delayCleanupBlobs(ctx, compactionLogBlobsToDelayCleanup, latestBlob.Timestamp); err != nil {
		return errors.Wrap(err, "unable to schedule delayed cleanup of blobs")
	}

	return nil
}

func (m *indexBlobManagerV0) findIndexBlobsToDelete(latestServerBlobTime time.Time, entries map[blob.ID]*compactionLogEntry, maxEventualConsistencySettleTime time.Duration) []blob.ID {
	tmp := map[blob.ID]bool{}

	for _, cl := range entries {
		// are the input index blobs in this compaction eligble for deletion?
		if age := latestServerBlobTime.Sub(cl.metadata.Timestamp); age < maxEventualConsistencySettleTime {
			m.log.Debugf("not deleting compacted index blob used as inputs for compaction %v, because it's too recent: %v < %v", cl.metadata.BlobID, age, maxEventualConsistencySettleTime)
			continue
		}

		for _, b := range cl.InputMetadata {
			m.log.Debugf("will delete old index %v compacted to %v", b, cl.OutputMetadata)

			tmp[b.BlobID] = true
		}
	}

	var result []blob.ID

	for k := range tmp {
		result = append(result, k)
	}

	return result
}

func (m *indexBlobManagerV0) findCompactionLogBlobsToDelayCleanup(compactionBlobs []blob.Metadata) []blob.ID {
	var result []blob.ID

	for _, cb := range compactionBlobs {
		m.log.Debugf("will delete compaction log blob %v", cb)
		result = append(result, cb.BlobID)
	}

	return result
}

func (m *indexBlobManagerV0) findBlobsToDelete(entries map[blob.ID]*cleanupEntry, maxEventualConsistencySettleTime time.Duration) (compactionLogs, cleanupBlobs []blob.ID) {
	for k, e := range entries {
		if e.age >= maxEventualConsistencySettleTime {
			compactionLogs = append(compactionLogs, e.BlobIDs...)
			cleanupBlobs = append(cleanupBlobs, k)
		}
	}

	return
}

func (m *indexBlobManagerV0) delayCleanupBlobs(ctx context.Context, blobIDs []blob.ID, cleanupScheduleTime time.Time) error {
	if len(blobIDs) == 0 {
		return nil
	}

	payload, err := json.Marshal(&cleanupEntry{
		BlobIDs:             blobIDs,
		CleanupScheduleTime: cleanupScheduleTime,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal cleanup log bytes")
	}

	if _, err := m.enc.encryptAndWriteBlob(ctx, payload, cleanupBlobPrefix, ""); err != nil {
		return errors.Wrap(err, "unable to cleanup log")
	}

	return nil
}

func (m *indexBlobManagerV0) deleteBlobsFromStorageAndCache(ctx context.Context, blobIDs []blob.ID) error {
	for _, blobID := range blobIDs {
		if err := m.st.DeleteBlob(ctx, blobID); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			m.log.Debugf("delete-blob failed %v %v", blobID, err)
			return errors.Wrapf(err, "unable to delete blob %v", blobID)
		}

		m.log.Debugf("delete-blob succeeded %v", blobID)
	}

	return nil
}

func (m *indexBlobManagerV0) cleanup(ctx context.Context, maxEventualConsistencySettleTime time.Duration) error {
	allCleanupBlobs, err := blob.ListAllBlobs(ctx, m.st, cleanupBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing cleanup blobs")
	}

	// determine latest storage write time of a cleanup blob
	var latestStorageWriteTimestamp time.Time

	for _, cb := range allCleanupBlobs {
		if cb.Timestamp.After(latestStorageWriteTimestamp) {
			latestStorageWriteTimestamp = cb.Timestamp
		}
	}

	// load cleanup entries and compute their age
	cleanupEntries, err := m.getCleanupEntries(ctx, latestStorageWriteTimestamp, allCleanupBlobs)
	if err != nil {
		return errors.Wrap(err, "error loading cleanup blobs")
	}

	// pick cleanup entries to delete that are old enough
	compactionLogsToDelete, cleanupBlobsToDelete := m.findBlobsToDelete(cleanupEntries, maxEventualConsistencySettleTime)

	if err := m.deleteBlobsFromStorageAndCache(ctx, compactionLogsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete cleanup blobs")
	}

	if err := m.deleteBlobsFromStorageAndCache(ctx, cleanupBlobsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete cleanup blobs")
	}

	m.flushCache(ctx)

	return nil
}

func (m *indexBlobManagerV0) getBlobsToCompact(indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlobs, verySmallBlobs []IndexBlobInfo

	var totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64

	var mediumSizedBlobCount int

	for _, b := range indexBlobs {
		if b.Length > int64(m.maxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedBlobs = append(nonCompactedBlobs, b)
		totalSizeNonCompactedBlobs += b.Length

		if b.Length < int64(m.maxPackSize/verySmallContentFraction) {
			verySmallBlobs = append(verySmallBlobs, b)
			totalSizeVerySmallBlobs += b.Length
		} else {
			mediumSizedBlobCount++
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		m.log.Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && mediumSizedBlobCount+1 < opt.MaxSmallBlobs {
		m.log.Debugf("compacting %v very small contents", len(verySmallBlobs))
		return verySmallBlobs
	}

	m.log.Debugf("compacting all %v non-compacted contents", len(nonCompactedBlobs))

	return nonCompactedBlobs
}

func (m *indexBlobManagerV0) compactIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 && opt.DropDeletedBefore.IsZero() && len(opt.DropContents) == 0 {
		return nil
	}

	bld := make(packIndexBuilder)

	var inputs, outputs []blob.Metadata

	for i, indexBlob := range indexBlobs {
		m.log.Debugf("compacting-entries[%v/%v] %v", i, len(indexBlobs), indexBlob)

		if err := m.addIndexBlobsToBuilder(ctx, bld, indexBlob); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}

		inputs = append(inputs, indexBlob.Metadata)
	}

	// after we built index map in memory, drop contents from it
	// we must do it after all input blobs have been merged, otherwise we may resurrect contents.
	m.dropContentsFromBuilder(bld, opt)

	var buf bytes.Buffer
	if err := bld.Build(&buf, m.indexVersion); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlob, err := m.writeIndexBlob(ctx, buf.Bytes(), "")
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	// compaction wrote index blob that's the same as one of the sources
	// it must be a no-op.
	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob.BlobID {
			m.log.Debugf("compaction-noop")
			return nil
		}
	}

	outputs = append(outputs, compactedIndexBlob)

	if err := m.registerCompaction(ctx, inputs, outputs, opt.maxEventualConsistencySettleTime()); err != nil {
		return errors.Wrap(err, "unable to register compaction")
	}

	return nil
}

func (m *indexBlobManagerV0) dropContentsFromBuilder(bld packIndexBuilder, opt CompactOptions) {
	for _, dc := range opt.DropContents {
		if _, ok := bld[dc]; ok {
			m.log.Debugf("manual-drop-from-index %v", dc)
			delete(bld, dc)
		}
	}

	if !opt.DropDeletedBefore.IsZero() {
		m.log.Debugf("drop-content-deleted-before %v", opt.DropDeletedBefore)

		for _, i := range bld {
			if i.GetDeleted() && i.Timestamp().Before(opt.DropDeletedBefore) {
				m.log.Debugf("drop-from-index-old-deleted %v %v", i.GetContentID(), i.Timestamp())
				delete(bld, i.GetContentID())
			}
		}

		m.log.Debugf("finished drop-content-deleted-before %v", opt.DropDeletedBefore)
	}
}

func (m *indexBlobManagerV0) addIndexBlobsToBuilder(ctx context.Context, bld packIndexBuilder, indexBlob IndexBlobInfo) error {
	data, err := m.getIndexBlob(ctx, indexBlob.BlobID)
	if err != nil {
		return errors.Wrapf(err, "error getting index %q", indexBlob.BlobID)
	}

	index, err := openPackIndex(bytes.NewReader(data), uint32(m.enc.crypter.Encryptor.Overhead()))
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlob)
	}

	_ = index.Iterate(AllIDs, func(i Info) error {
		bld.Add(i)
		return nil
	})

	return nil
}

func blobsOlderThan(m []blob.Metadata, cutoffTime time.Time) []blob.Metadata {
	var res []blob.Metadata

	for _, m := range m {
		if !m.Timestamp.After(cutoffTime) {
			res = append(res, m)
		}
	}

	return res
}

func (m *indexBlobManagerV0) removeCompactedIndexes(bimap map[blob.ID]*IndexBlobInfo, compactionLogs map[blob.ID]*compactionLogEntry, markAsSuperseded bool) {
	var validCompactionLogs []*compactionLogEntry

	for _, cl := range compactionLogs {
		// only process compaction logs for which we have found all the outputs.
		haveAllOutputs := true

		for _, o := range cl.OutputMetadata {
			if bimap[o.BlobID] == nil {
				haveAllOutputs = false

				m.log.Debugf("blob %v referenced by compaction log is not found", o.BlobID)

				break
			}
		}

		if haveAllOutputs {
			validCompactionLogs = append(validCompactionLogs, cl)
		}
	}

	// now remove all inputs from the set if there's a valid compaction log entry with all the outputs.
	for _, cl := range validCompactionLogs {
		for _, ib := range cl.InputMetadata {
			if md := bimap[ib.BlobID]; md != nil && md.Superseded == nil {
				m.log.Debugf("ignore-index-blob %v compacted to %v", ib, cl.OutputMetadata)

				if markAsSuperseded {
					md.Superseded = cl.OutputMetadata
				} else {
					delete(bimap, ib.BlobID)
				}
			}
		}
	}
}
