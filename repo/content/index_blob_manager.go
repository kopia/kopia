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

type indexBlobManager struct {
	st             blob.Storage
	hasher         hashing.HashFunc
	encryptor      encryption.Encryptor
	listCache      *listCache
	ownWritesCache ownWritesCache
	timeNow        func() time.Time
	indexBlobCache contentCache
}

func (m *indexBlobManager) listEffectiveIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
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
		return nil, errors.Wrap(err, "error reading compaction log compactionLogs")
	}

	// remove entries from indexMap that have been compacted and replaced by other indexes.
	removeCompactedIndexes(ctx, indexMap, compactionLogs, includeInactive)

	var results []IndexBlobInfo
	for _, v := range indexMap {
		results = append(results, *v)
	}

	return results, nil
}

func (m *indexBlobManager) registerCompaction(ctx context.Context, inputs, outputs []blob.ID) error {
	logEntryBytes, err := json.Marshal(&compactionLogEntry{
		InputBlobs:  inputs,
		OutputBlobs: outputs,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobID, err := m.encryptAndWriteBlob(ctx, logEntryBytes, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	formatLog(ctx).Debugf("compacted indexes %v into %v and wrote log %v", inputs, outputs, compactionLogBlobID)

	if err := m.deleteOldIndexBlobs(ctx, compactionLogBlobID); err != nil {
		return errors.Wrap(err, "error deleting old index blobs")
	}

	return nil
}

func (m *indexBlobManager) getIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
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

func (m *indexBlobManager) verifyChecksum(data, contentID []byte) error {
	var hashOutput [maxHashSize]byte

	expected := m.hasher(hashOutput[:0], data)
	expected = expected[len(expected)-aes.BlockSize:]

	if !bytes.HasSuffix(contentID, expected) {
		return errors.Errorf("invalid checksum for blob %x, expected %x", contentID, expected)
	}

	return nil
}

func (m *indexBlobManager) encryptAndWriteBlob(ctx context.Context, data []byte, prefix blob.ID) (blob.ID, error) {
	var hashOutput [maxHashSize]byte

	hash := m.hasher(hashOutput[:0], data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return "", err
	}

	data2, err := m.encryptor.Encrypt(nil, data, iv)
	if err != nil {
		return "", err
	}

	m.listCache.deleteListCache(prefix)

	if err := m.st.PutBlob(ctx, blobID, gather.FromSlice(data2)); err != nil {
		return "", err
	}

	if err := m.ownWritesCache.add(ctx, blob.Metadata{
		BlobID:    blobID,
		Length:    int64(len(data2)),
		Timestamp: m.timeNow(),
	}); err != nil {
		log(ctx).Warningf("unable to cache own write: %v", err)
	}

	return blobID, nil
}

func (m *indexBlobManager) getCompactionLogEntries(ctx context.Context, blobs []blob.Metadata) (map[blob.ID]*compactionLogEntry, error) {
	results := map[blob.ID]*compactionLogEntry{}

	for _, cb := range blobs {
		data, err := m.getIndexBlob(ctx, cb.BlobID)
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

func (m *indexBlobManager) deleteOldIndexBlobs(ctx context.Context, latestBlobID blob.ID) error {
	allCompactionLogBlobs, err := m.listCache.listIndexBlobs(ctx, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing compaction log blobs")
	}

	// look for server-assigned timestamp of the compaction log entry we just wrote as a reference.
	// we're assuming server-generated timestamps are somewhat reasonable and time is moving
	latestBlobTime := blobTime(allCompactionLogBlobs, latestBlobID)
	if latestBlobTime.IsZero() {
		formatLog(ctx).Warningf("compaction log blob %q was not found in list results due to eventual consistency, ignoring", latestBlobID)
		return nil
	}

	fetchCompactionLogs := blobsOlderThan(allCompactionLogBlobs, latestBlobTime.Add(-blobDeleteSafeTime))

	entries, err := m.getCompactionLogEntries(ctx, fetchCompactionLogs)
	if err != nil {
		return errors.Wrap(err, "unable to get compaction log entries")
	}

	oldCompactedBlobs := map[blob.ID]bool{}

	for _, cl := range entries {
		for _, b := range cl.InputBlobs {
			log(ctx).Debugf("will delete old index %q compacted to %v", b, cl.OutputBlobs)

			oldCompactedBlobs[b] = true
		}
	}

	for cb := range oldCompactedBlobs {
		log(ctx).Debugf("deleting compacted blob %v", cb)

		if err := m.st.DeleteBlob(ctx, cb); err != nil && err != blob.ErrBlobNotFound {
			formatLog(ctx).Warningf("unable to delete compacted blob %v, %v", cb, err)
		}
	}

	for _, cb := range fetchCompactionLogs {
		log(ctx).Debugf("deleting compaction log blob %v", cb)

		if err := m.st.DeleteBlob(ctx, cb.BlobID); err != nil && err != blob.ErrBlobNotFound {
			formatLog(ctx).Warningf("unable to delete compaction log blob %v, %v", cb.BlobID, err)
		}
	}

	m.listCache.deleteListCache(indexBlobPrefix)
	m.listCache.deleteListCache(compactionLogBlobPrefix)

	return nil
}

func blobsOlderThan(m []blob.Metadata, cutoffTime time.Time) []blob.Metadata {
	var res []blob.Metadata

	for _, m := range m {
		if m.Timestamp.Before(cutoffTime) {
			res = append(res, m)
		}
	}

	return res
}

func blobTime(m []blob.Metadata, blobID blob.ID) time.Time {
	for _, m := range m {
		if m.BlobID == blobID {
			return m.Timestamp
		}
	}

	return time.Time{}
}

func getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { // nolint:gocritic
		s = s[0:p]
	}

	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):]))
}
