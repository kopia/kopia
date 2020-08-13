package content

import (
	"bytes"
	"context"
	"crypto/aes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// indexBlobManager is the API of index blob manager as used by content manager.
type indexBlobManager interface {
	writeIndexBlob(ctx context.Context, data []byte) (blob.Metadata, error)
	listIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error)
	getIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error)
	registerCompaction(ctx context.Context, inputs, outputs []blob.Metadata) error
	cleanup(ctx context.Context) error
	flushCache()
}

const (
	compactionLogBlobPrefix              = "m"
	cleanupBlobPrefix                    = "l"
	defaultEventualConsistencySettleTime = 1 * time.Hour
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
	st                               blob.Storage
	hasher                           hashing.HashFunc
	encryptor                        encryption.Encryptor
	listCache                        *listCache
	ownWritesCache                   ownWritesCache
	timeNow                          func() time.Time
	indexBlobCache                   contentCache
	maxEventualConsistencySettleTime time.Duration
}

func (m *indexBlobManagerImpl) listIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	compactionLogMetadata, err := m.listCache.listBlobs(ctx, compactionLogBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing compaction log entries")
	}

	compactionLogMetadata, err = m.ownWritesCache.merge(ctx, compactionLogBlobPrefix, compactionLogMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error merging local writes for compaction log entries")
	}

	storageIndexBlobs, err := m.listCache.listBlobs(ctx, indexBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing index blobs")
	}

	storageIndexBlobs, err = m.ownWritesCache.merge(ctx, indexBlobPrefix, storageIndexBlobs)
	if err != nil {
		return nil, errors.Wrap(err, "error merging local writes for index blobs")
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

	return results, nil
}

func (m *indexBlobManagerImpl) flushCache() {
	m.listCache.deleteListCache(indexBlobPrefix)
	m.listCache.deleteListCache(compactionLogBlobPrefix)
}

func (m *indexBlobManagerImpl) registerCompaction(ctx context.Context, inputs, outputs []blob.Metadata) error {
	logEntryBytes, err := json.Marshal(&compactionLogEntry{
		InputMetadata:  inputs,
		OutputMetadata: outputs,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobMetadata, err := m.encryptAndWriteBlob(ctx, logEntryBytes, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	formatLog(ctx).Debugf("compacted indexes %v into %v and wrote log %v", inputs, outputs, compactionLogBlobMetadata)

	if err := m.deleteOldBlobs(ctx, compactionLogBlobMetadata); err != nil {
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
		return nil, err
	}

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return nil, err
	}

	payload, err = m.encryptor.Decrypt(nil, payload, iv)

	if err != nil {
		return nil, errors.Wrap(err, "decrypt error")
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := m.verifyChecksum(payload, iv); err != nil {
		return nil, err
	}

	return payload, nil
}

func (m *indexBlobManagerImpl) verifyChecksum(data, contentID []byte) error {
	var hashOutput [maxHashSize]byte

	expected := m.hasher(hashOutput[:0], data)
	expected = expected[len(expected)-aes.BlockSize:]

	if !bytes.HasSuffix(contentID, expected) {
		return errors.Errorf("invalid checksum for blob %x, expected %x", contentID, expected)
	}

	return nil
}

func (m *indexBlobManagerImpl) writeIndexBlob(ctx context.Context, data []byte) (blob.Metadata, error) {
	return m.encryptAndWriteBlob(ctx, data, indexBlobPrefix)
}

func (m *indexBlobManagerImpl) encryptAndWriteBlob(ctx context.Context, data []byte, prefix blob.ID) (blob.Metadata, error) {
	var hashOutput [maxHashSize]byte

	hash := m.hasher(hashOutput[:0], data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return blob.Metadata{}, err
	}

	data2, err := m.encryptor.Encrypt(nil, data, iv)
	if err != nil {
		return blob.Metadata{}, err
	}

	m.listCache.deleteListCache(prefix)

	err = m.st.PutBlob(ctx, blobID, gather.FromSlice(data2))
	if err != nil {
		return blob.Metadata{}, err
	}

	bm, err := m.st.GetMetadata(ctx, blobID)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "unable to get blob metadata")
	}

	if err := m.ownWritesCache.add(ctx, bm); err != nil {
		log(ctx).Warningf("unable to cache own write: %v", err)
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

func (m *indexBlobManagerImpl) deleteOldBlobs(ctx context.Context, latestBlob blob.Metadata) error {
	allCompactionLogBlobs, err := m.listCache.listBlobs(ctx, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing compaction log blobs")
	}

	// look for server-assigned timestamp of the compaction log entry we just wrote as a reference.
	// we're assuming server-generated timestamps are somewhat reasonable and time is moving
	compactionLogServerTimeCutoff := latestBlob.Timestamp.Add(-m.maxEventualConsistencySettleTime)
	compactionBlobs := blobsOlderThan(allCompactionLogBlobs, compactionLogServerTimeCutoff)

	log(ctx).Debugf("fetching %v/%v compaction logs older than %v", len(compactionBlobs), len(allCompactionLogBlobs), compactionLogServerTimeCutoff)

	compactionBlobEntries, err := m.getCompactionLogEntries(ctx, compactionBlobs)
	if err != nil {
		return errors.Wrap(err, "unable to get compaction log entries")
	}

	indexBlobsToDelete := m.findIndexBlobsToDelete(ctx, latestBlob.Timestamp, compactionBlobEntries)

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

func (m *indexBlobManagerImpl) findIndexBlobsToDelete(ctx context.Context, latestServerBlobTime time.Time, entries map[blob.ID]*compactionLogEntry) []blob.ID {
	tmp := map[blob.ID]bool{}

	for _, cl := range entries {
		// are the input index blobs in this compaction eligble for deletion?
		if age := latestServerBlobTime.Sub(cl.metadata.Timestamp); age < m.maxEventualConsistencySettleTime {
			log(ctx).Debugf("not deleting compacted index blob used as inputs for compaction %v, because it's too recent: %v < %v", cl.metadata.BlobID, age, m.maxEventualConsistencySettleTime)
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

func (m *indexBlobManagerImpl) findBlobsToDelete(entries map[blob.ID]*cleanupEntry) (compactionLogs, cleanupBlobs []blob.ID) {
	for k, e := range entries {
		if e.age > m.maxEventualConsistencySettleTime {
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

	if _, err := m.encryptAndWriteBlob(ctx, payload, cleanupBlobPrefix); err != nil {
		return errors.Wrap(err, "unable to cleanup log")
	}

	return nil
}

func (m *indexBlobManagerImpl) deleteBlobsFromStorageAndCache(ctx context.Context, blobIDs []blob.ID) error {
	for _, blobID := range blobIDs {
		if err := m.st.DeleteBlob(ctx, blobID); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			return errors.Wrapf(err, "unable to delete blob %v", blobID)
		}

		if err := m.ownWritesCache.delete(ctx, blobID); err != nil {
			return errors.Wrapf(err, "unable to delete blob %v from own-writes cache", blobID)
		}
	}

	return nil
}

func (m *indexBlobManagerImpl) cleanup(ctx context.Context) error {
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
	compactionLogsToDelete, cleanupBlobsToDelete := m.findBlobsToDelete(cleanupEntries)

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

func getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { // nolint:gocritic
		s = s[0:p]
	}

	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):]))
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
				log(ctx).Debugf("ignoring index blob %v (%v) because it's been compacted to %v", ib, md.Timestamp, cl.OutputMetadata)

				if markAsSuperseded {
					md.Superseded = cl.OutputMetadata
				} else {
					delete(m, ib.BlobID)
				}
			}
		}
	}
}
