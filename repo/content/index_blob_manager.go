package content

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// indexBlobManager is the API of index blob manager as used by content manager.
type indexBlobManager interface {
	writeIndexBlob(ctx context.Context, data []byte, sessionID SessionID) (blob.Metadata, error)
	listIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error)
	getIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error)
	registerCompaction(ctx context.Context, inputs, outputs []blob.Metadata, maxEventualConsistencySettleTime time.Duration) error
	cleanup(ctx context.Context, maxEventualConsistencySettleTime time.Duration) error
	flushCache()
}

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

type indexBlobManagerImpl struct {
	st             blob.Storage
	hasher         hashing.HashFunc
	encryptor      encryption.Encryptor
	listCache      *listCache
	ownWritesCache ownWritesCache
	timeNow        func() time.Time
	indexBlobCache contentCache
}

func (m *indexBlobManagerImpl) listAndMergeOwnWrites(ctx context.Context, prefix blob.ID) ([]blob.Metadata, error) {
	found, err := m.listCache.listBlobs(ctx, prefix)
	if err != nil {
		return nil, errors.Wrapf(err, "error listing %v blobs", prefix)
	}

	merged, err := m.ownWritesCache.merge(ctx, prefix, found)
	if err != nil {
		return nil, errors.Wrapf(err, "error merging local writes for %v blobs", prefix)
	}

	return merged, nil
}

func (m *indexBlobManagerImpl) listIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	var compactionLogMetadata, storageIndexBlobs []blob.Metadata

	var eg errgroup.Group

	// list index and cleanup blobs in parallel and merge with own-writes cache.
	eg.Go(func() error {
		v, err := m.listAndMergeOwnWrites(ctx, compactionLogBlobPrefix)
		compactionLogMetadata = v
		return err
	})

	eg.Go(func() error {
		v, err := m.listAndMergeOwnWrites(ctx, indexBlobPrefix)
		storageIndexBlobs = v
		return err
	})

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "error listing indexes")
	}

	for i, sib := range storageIndexBlobs {
		formatLog(ctx).Debugf("found-index-blobs[%v] = %v", i, sib)
	}

	for i, clm := range compactionLogMetadata {
		formatLog(ctx).Debugf("found-compaction-blobs[%v] %v", i, clm)
	}

	indexMap := map[blob.ID]*IndexBlobInfo{}
	addBlobsToIndex(indexMap, storageIndexBlobs)

	compactionLogs, err := m.getCompactionLogEntries(ctx, compactionLogMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error reading compaction log")
	}

	// remove entries from indexMap that have been compacted and replaced by other indexes.
	removeCompactedIndexes(ctx, indexMap, compactionLogs, includeInactive)

	var results []IndexBlobInfo
	for _, v := range indexMap {
		results = append(results, *v)
	}

	for i, res := range results {
		formatLog(ctx).Debugf("active-index-blobs[%v] = %v", i, res)
	}

	return results, nil
}

func (m *indexBlobManagerImpl) flushCache() {
	m.listCache.deleteListCache(indexBlobPrefix)
	m.listCache.deleteListCache(compactionLogBlobPrefix)
}

func (m *indexBlobManagerImpl) registerCompaction(ctx context.Context, inputs, outputs []blob.Metadata, maxEventualConsistencySettleTime time.Duration) error {
	logEntryBytes, err := json.Marshal(&compactionLogEntry{
		InputMetadata:  inputs,
		OutputMetadata: outputs,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobMetadata, err := m.encryptAndWriteBlob(ctx, logEntryBytes, compactionLogBlobPrefix, "")
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	for i, input := range inputs {
		formatLog(ctx).Debugf("compacted-input[%v/%v] %v", i, len(inputs), input)
	}

	for i, output := range outputs {
		formatLog(ctx).Debugf("compacted-output[%v/%v] %v", i, len(outputs), output)
	}

	formatLog(ctx).Debugf("compaction-log %v %v", compactionLogBlobMetadata.BlobID, compactionLogBlobMetadata.Timestamp)

	if err := m.deleteOldBlobs(ctx, compactionLogBlobMetadata, maxEventualConsistencySettleTime); err != nil {
		return errors.Wrap(err, "error deleting old index blobs")
	}

	return nil
}

func (m *indexBlobManagerImpl) getIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
	return m.getEncryptedBlob(ctx, blobID)
}

func (m *indexBlobManagerImpl) getEncryptedBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
	payload, err := m.indexBlobCache.getContent(ctx, cacheKey(blobID), blobID, 0, -1)
	if err != nil {
		return nil, errors.Wrap(err, "getContent")
	}

	return decryptFullBlob(m.hasher, m.encryptor, payload, blobID)
}

func (m *indexBlobManagerImpl) writeIndexBlob(ctx context.Context, data []byte, sessionID SessionID) (blob.Metadata, error) {
	return m.encryptAndWriteBlob(ctx, data, indexBlobPrefix, sessionID)
}

func (m *indexBlobManagerImpl) encryptAndWriteBlob(ctx context.Context, data []byte, prefix blob.ID, sessionID SessionID) (blob.Metadata, error) {
	blobID, data2, err := encryptFullBlob(m.hasher, m.encryptor, data, prefix, sessionID)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error encrypting")
	}

	m.listCache.deleteListCache(prefix)

	err = m.st.PutBlob(ctx, blobID, gather.FromSlice(data2))
	if err != nil {
		formatLog(ctx).Debugf("write-index-blob %v failed %v", blobID, err)
		return blob.Metadata{}, errors.Wrapf(err, "error writing blob %v", blobID)
	}

	bm, err := m.st.GetMetadata(ctx, blobID)
	if err != nil {
		formatLog(ctx).Debugf("write-index-blob-get-metadata %v failed %v", blobID, err)
		return blob.Metadata{}, errors.Wrap(err, "unable to get blob metadata")
	}

	formatLog(ctx).Debugf("write-index-blob %v %v %v", blobID, bm.Length, bm.Timestamp)

	if err := m.ownWritesCache.add(ctx, bm); err != nil {
		formatLog(ctx).Errorf("own-writes-cache failure: %v", err)
	}

	return bm, nil
}

func (m *indexBlobManagerImpl) getCompactionLogEntries(ctx context.Context, blobs []blob.Metadata) (map[blob.ID]*compactionLogEntry, error) {
	results := map[blob.ID]*compactionLogEntry{}

	for _, cb := range blobs {
		data, err := m.getEncryptedBlob(ctx, cb.BlobID)

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

func (m *indexBlobManagerImpl) getCleanupEntries(ctx context.Context, latestServerBlobTime time.Time, blobs []blob.Metadata) (map[blob.ID]*cleanupEntry, error) {
	results := map[blob.ID]*cleanupEntry{}

	for _, cb := range blobs {
		data, err := m.getEncryptedBlob(ctx, cb.BlobID)

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

func (m *indexBlobManagerImpl) deleteOldBlobs(ctx context.Context, latestBlob blob.Metadata, maxEventualConsistencySettleTime time.Duration) error {
	allCompactionLogBlobs, err := m.listCache.listBlobs(ctx, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing compaction log blobs")
	}

	// look for server-assigned timestamp of the compaction log entry we just wrote as a reference.
	// we're assuming server-generated timestamps are somewhat reasonable and time is moving
	compactionLogServerTimeCutoff := latestBlob.Timestamp.Add(-maxEventualConsistencySettleTime)
	compactionBlobs := blobsOlderThan(allCompactionLogBlobs, compactionLogServerTimeCutoff)

	log(ctx).Debugf("fetching %v/%v compaction logs older than %v", len(compactionBlobs), len(allCompactionLogBlobs), compactionLogServerTimeCutoff)

	compactionBlobEntries, err := m.getCompactionLogEntries(ctx, compactionBlobs)
	if err != nil {
		return errors.Wrap(err, "unable to get compaction log entries")
	}

	indexBlobsToDelete := m.findIndexBlobsToDelete(ctx, latestBlob.Timestamp, compactionBlobEntries, maxEventualConsistencySettleTime)

	// note that we must always delete index blobs first before compaction logs
	// otherwise we may inadvertedly resurrect an index blob that should have been removed.
	if err := m.deleteBlobsFromStorageAndCache(ctx, indexBlobsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete compaction logs")
	}

	compactionLogBlobsToDelayCleanup := m.findCompactionLogBlobsToDelayCleanup(ctx, compactionBlobs)

	if err := m.delayCleanupBlobs(ctx, compactionLogBlobsToDelayCleanup, latestBlob.Timestamp); err != nil {
		return errors.Wrap(err, "unable to schedule delayed cleanup of blobs")
	}

	return nil
}

func (m *indexBlobManagerImpl) findIndexBlobsToDelete(ctx context.Context, latestServerBlobTime time.Time, entries map[blob.ID]*compactionLogEntry, maxEventualConsistencySettleTime time.Duration) []blob.ID {
	tmp := map[blob.ID]bool{}

	for _, cl := range entries {
		// are the input index blobs in this compaction eligble for deletion?
		if age := latestServerBlobTime.Sub(cl.metadata.Timestamp); age < maxEventualConsistencySettleTime {
			log(ctx).Debugf("not deleting compacted index blob used as inputs for compaction %v, because it's too recent: %v < %v", cl.metadata.BlobID, age, maxEventualConsistencySettleTime)
			continue
		}

		for _, b := range cl.InputMetadata {
			log(ctx).Debugf("will delete old index %v compacted to %v", b, cl.OutputMetadata)

			tmp[b.BlobID] = true
		}
	}

	var result []blob.ID

	for k := range tmp {
		result = append(result, k)
	}

	return result
}

func (m *indexBlobManagerImpl) findCompactionLogBlobsToDelayCleanup(ctx context.Context, compactionBlobs []blob.Metadata) []blob.ID {
	var result []blob.ID

	for _, cb := range compactionBlobs {
		log(ctx).Debugf("will delete compaction log blob %v", cb)
		result = append(result, cb.BlobID)
	}

	return result
}

func (m *indexBlobManagerImpl) findBlobsToDelete(entries map[blob.ID]*cleanupEntry, maxEventualConsistencySettleTime time.Duration) (compactionLogs, cleanupBlobs []blob.ID) {
	for k, e := range entries {
		if e.age >= maxEventualConsistencySettleTime {
			compactionLogs = append(compactionLogs, e.BlobIDs...)
			cleanupBlobs = append(cleanupBlobs, k)
		}
	}

	return
}

func (m *indexBlobManagerImpl) delayCleanupBlobs(ctx context.Context, blobIDs []blob.ID, cleanupScheduleTime time.Time) error {
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

	if _, err := m.encryptAndWriteBlob(ctx, payload, cleanupBlobPrefix, ""); err != nil {
		return errors.Wrap(err, "unable to cleanup log")
	}

	return nil
}

func (m *indexBlobManagerImpl) deleteBlobsFromStorageAndCache(ctx context.Context, blobIDs []blob.ID) error {
	for _, blobID := range blobIDs {
		if err := m.st.DeleteBlob(ctx, blobID); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			formatLog(ctx).Debugf("delete-blob failed %v %v", blobID, err)
			return errors.Wrapf(err, "unable to delete blob %v", blobID)
		}

		formatLog(ctx).Debugf("delete-blob succeeded %v", blobID)

		if err := m.ownWritesCache.delete(ctx, blobID); err != nil {
			return errors.Wrapf(err, "unable to delete blob %v from own-writes cache", blobID)
		}
	}

	return nil
}

func (m *indexBlobManagerImpl) cleanup(ctx context.Context, maxEventualConsistencySettleTime time.Duration) error {
	allCleanupBlobs, err := m.listCache.listBlobs(ctx, cleanupBlobPrefix)
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

	m.flushCache()

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

func removeCompactedIndexes(ctx context.Context, m map[blob.ID]*IndexBlobInfo, compactionLogs map[blob.ID]*compactionLogEntry, markAsSuperseded bool) {
	var validCompactionLogs []*compactionLogEntry

	for _, cl := range compactionLogs {
		// only process compaction logs for which we have found all the outputs.
		haveAllOutputs := true

		for _, o := range cl.OutputMetadata {
			if m[o.BlobID] == nil {
				haveAllOutputs = false

				log(ctx).Debugf("blob %v referenced by compaction log is not found", o.BlobID)

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
			if md := m[ib.BlobID]; md != nil && md.Superseded == nil {
				formatLog(ctx).Debugf("ignore-index-blob %v compacted to %v", ib, cl.OutputMetadata)

				if markAsSuperseded {
					md.Superseded = cl.OutputMetadata
				} else {
					delete(m, ib.BlobID)
				}
			}
		}
	}
}
