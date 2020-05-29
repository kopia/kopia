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
	flushCache()
}

const (
	compactionLogBlobPrefix = "m"
)

// compactionLogEntry represents contents of compaction log entry stored in `m` blob.
type compactionLogEntry struct {
	// list of input blob names that were compacted together.
	InputBlobs []blob.Metadata `json:"inputMetadata"`

	// list of blobs that are results of compaction.
	OutputBlobs []blob.Metadata `json:"outputMetadata"`
}

type indexBlobManagerImpl struct {
	st                            blob.Storage
	hasher                        hashing.HashFunc
	encryptor                     encryption.Encryptor
	listCache                     *listCache
	ownWritesCache                ownWritesCache
	timeNow                       func() time.Time
	indexBlobCache                contentCache
	minIndexBlobDeleteAge         time.Duration
	minCompactionLogBlobDeleteAge time.Duration
}

func (m *indexBlobManagerImpl) listIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	compactionLogMetadata, err := m.listCache.listIndexBlobs(ctx, compactionLogBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing compaction log entries")
	}

	compactionLogMetadata, err = m.ownWritesCache.merge(ctx, compactionLogBlobPrefix, compactionLogMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error merging local writes for compaction log entries")
	}

	storageIndexBlobs, err := m.listCache.listIndexBlobs(ctx, indexBlobPrefix)
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
		InputBlobs:  inputs,
		OutputBlobs: outputs,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobMetadata, err := m.encryptAndWriteBlob(ctx, logEntryBytes, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	formatLog(ctx).Debugf("compacted indexes %v into %v and wrote log %v", inputs, outputs, compactionLogBlobMetadata)

	if err := m.deleteOldIndexBlobs(ctx, compactionLogBlobMetadata); err != nil {
		return errors.Wrap(err, "error deleting old index blobs")
	}

	return nil
}

func (m *indexBlobManagerImpl) getIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
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
		data, err := m.getIndexBlob(ctx, cb.BlobID)

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

		results[cb.BlobID] = le
	}

	return results, nil
}

func (m *indexBlobManagerImpl) deleteOldIndexBlobs(ctx context.Context, latestBlob blob.Metadata) error {
	if m.minCompactionLogBlobDeleteAge <= m.minIndexBlobDeleteAge {
		return errors.Errorf("configuration error - compaction log deletion age (%v) must be greater than index blob deletion age (%v)", m.minCompactionLogBlobDeleteAge, m.minIndexBlobDeleteAge)
	}

	allCompactionLogBlobs, err := m.listCache.listIndexBlobs(ctx, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing compaction log blobs")
	}

	// look for server-assigned timestamp of the compaction log entry we just wrote as a reference.
	// we're assuming server-generated timestamps are somewhat reasonable and time is moving
	compactionLogServerTimeCutoff := latestBlob.Timestamp.Add(-m.minIndexBlobDeleteAge)
	compactionBlobs := blobsOlderThan(allCompactionLogBlobs, compactionLogServerTimeCutoff)

	log(ctx).Debugf("fetching %v/%v compaction logs older than %v", len(compactionBlobs), len(allCompactionLogBlobs), compactionLogServerTimeCutoff)

	compactionBlobEntries, err := m.getCompactionLogEntries(ctx, compactionBlobs)
	if err != nil {
		return errors.Wrap(err, "unable to get compaction log entries")
	}

	indexBlobsToDelete := m.findIndexBlobsToDelete(ctx, latestBlob.Timestamp, compactionBlobEntries)
	compactionLogBlobsToDelete := m.findCompactionLogBlobsToDelete(ctx, latestBlob.Timestamp, compactionBlobs)

	// note that we must always delete index blobs first before compaction logs
	// otherwise we may inadvertedly resurrect an index blob that should have been removed.
	if err := m.deleteBlobsFromStorageAndCache(ctx, indexBlobsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete compaction logs")
	}

	if err := m.deleteBlobsFromStorageAndCache(ctx, compactionLogBlobsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete compaction logs")
	}

	m.flushCache()

	return nil
}

func (m *indexBlobManagerImpl) findIndexBlobsToDelete(ctx context.Context, latestServerBlobTime time.Time, entries map[blob.ID]*compactionLogEntry) []blob.ID {
	tmp := map[blob.ID]bool{}

	for _, cl := range entries {
		for _, b := range cl.InputBlobs {
			if age := latestServerBlobTime.Sub(b.Timestamp); age < m.minIndexBlobDeleteAge {
				log(ctx).Debugf("not deleting compacted index blob %v, because it's too recent: %v < %v", b.BlobID, age, m.minIndexBlobDeleteAge)
				continue
			}

			log(ctx).Debugf("will delete old index %v compacted to %v", b, cl.OutputBlobs)

			tmp[b.BlobID] = true
		}
	}

	var result []blob.ID

	for k := range tmp {
		result = append(result, k)
	}

	return result
}

func (m *indexBlobManagerImpl) findCompactionLogBlobsToDelete(ctx context.Context, latestServerBlobTime time.Time, compactionBlobs []blob.Metadata) []blob.ID {
	var result []blob.ID

	for _, cb := range compactionBlobs {
		if age := latestServerBlobTime.Sub(cb.Timestamp); age < m.minCompactionLogBlobDeleteAge {
			log(ctx).Debugf("not deleting compaction log blob %v, because it's too recent: %v < %v", cb, age, m.minCompactionLogBlobDeleteAge)
			continue
		}

		log(ctx).Debugf("will delete compaction log blob %v", cb)
		result = append(result, cb.BlobID)
	}

	return result
}

func (m *indexBlobManagerImpl) deleteBlobsFromStorageAndCache(ctx context.Context, blobIDs []blob.ID) error {
	for _, blobID := range blobIDs {
		if err := m.st.DeleteBlob(ctx, blobID); err != nil && err != blob.ErrBlobNotFound {
			return errors.Wrapf(err, "unable to delete blob %v", blobID)
		}

		if err := m.ownWritesCache.delete(ctx, blobID); err != nil {
			return errors.Wrapf(err, "unable to delete blob %v from own-writes cache", blobID)
		}
	}

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

		for _, o := range cl.OutputBlobs {
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
		for _, ib := range cl.InputBlobs {
			if md := m[ib.BlobID]; md != nil && md.Superseded == nil {
				log(ctx).Debugf("ignoring index blob %v (%v) because it's been compacted to %v", ib, md.Timestamp, cl.OutputBlobs)

				if markAsSuperseded {
					md.Superseded = cl.OutputBlobs
				} else {
					delete(m, ib.BlobID)
				}
			}
		}
	}
}
