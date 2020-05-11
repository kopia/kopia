package content

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const verySmallContentFraction = 20 // blobs less than 1/verySmallContentFraction of maxPackSize are considered 'very small'

var autoCompactionOptions = CompactOptions{
	MaxSmallBlobs: 4 * parallelFetches, // nolint:gomnd
}

const (
	compactionLogBlobPrefix = "m"
	blobDeleteSafeTime      = 1 * time.Hour
)

// compactionLogEntry represents contents of compaction log entry stored in `m` blob.
type compactionLogEntry struct {
	// random ID which ensures that each compaction log entry is unique
	RandomID []byte `json:"randomID"`

	// list of input blob names that were compacted together.
	InputBlobs []blob.ID `json:"inputs"`

	// list of blobs that are results of compaction.
	OutputBlobs []blob.ID `json:"outputs"`
}

// CompactOptions provides options for compaction
type CompactOptions struct {
	MaxSmallBlobs     int
	AllIndexes        bool
	DropDeletedBefore time.Time
}

// CompactIndexes performs compaction of index blobs ensuring that # of small index blobs is below opt.maxSmallBlobs
func (bm *Manager) CompactIndexes(ctx context.Context, opt CompactOptions) error {
	log(ctx).Debugf("CompactIndexes(%+v)", opt)

	bm.lock()
	defer bm.unlock()

	indexBlobs, _, err := bm.loadPackIndexesUnlocked(ctx)
	if err != nil {
		return errors.Wrap(err, "error loading indexes")
	}

	contentsToCompact := bm.getContentsToCompact(ctx, indexBlobs, opt)

	if err := bm.compactAndDeleteIndexBlobs(ctx, contentsToCompact, opt); err != nil {
		log(ctx).Warningf("error performing quick compaction: %v", err)
	}

	return nil
}

func (bm *Manager) getContentsToCompact(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) []IndexBlobInfo {
	var nonCompactedBlobs, verySmallBlobs []IndexBlobInfo

	var totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64

	var mediumSizedBlobCount int

	for _, b := range indexBlobs {
		if b.Length > int64(bm.maxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedBlobs = append(nonCompactedBlobs, b)
		totalSizeNonCompactedBlobs += b.Length

		if b.Length < int64(bm.maxPackSize/verySmallContentFraction) {
			verySmallBlobs = append(verySmallBlobs, b)
			totalSizeVerySmallBlobs += b.Length
		} else {
			mediumSizedBlobCount++
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		formatLog(ctx).Debugf("no small contents to compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && mediumSizedBlobCount+1 < opt.MaxSmallBlobs {
		formatLog(ctx).Debugf("compacting %v very small contents", len(verySmallBlobs))
		return verySmallBlobs
	}

	formatLog(ctx).Debugf("compacting all %v non-compacted contents", len(nonCompactedBlobs))

	return nonCompactedBlobs
}

func (bm *Manager) compactAndDeleteIndexBlobs(ctx context.Context, indexBlobs []IndexBlobInfo, opt CompactOptions) error {
	if len(indexBlobs) <= 1 {
		return nil
	}

	formatLog(ctx).Debugf("compacting %v contents", len(indexBlobs))

	t0 := time.Now() // allow:no-inject-time
	bld := make(packIndexBuilder)

	var logEntry compactionLogEntry

	// generate random log entry ID to ensure that when it is hashed later,
	// we don't get any collisions.
	logEntry.RandomID = make([]byte, 32)
	if _, err := rand.Read(logEntry.RandomID); err != nil {
		return errors.Wrap(err, "unable to get random log entry ID")
	}

	for _, indexBlob := range indexBlobs {
		if err := bm.addIndexBlobsToBuilder(ctx, bld, indexBlob, opt); err != nil {
			return errors.Wrap(err, "error adding index to builder")
		}

		logEntry.InputBlobs = append(logEntry.InputBlobs, indexBlob.BlobID)
	}

	var buf bytes.Buffer
	if err := bld.Build(&buf); err != nil {
		return errors.Wrap(err, "unable to build an index")
	}

	compactedIndexBlob, err := bm.writePackIndexesNew(ctx, buf.Bytes())
	if err != nil {
		return errors.Wrap(err, "unable to write compacted indexes")
	}

	// compaction wrote index blob that's the same as one of the sources
	// it must be a no-op.
	for _, indexBlob := range indexBlobs {
		if indexBlob.BlobID == compactedIndexBlob {
			formatLog(ctx).Debugf("compaction was a no-op")
			return nil
		}
	}

	logEntry.OutputBlobs = append(logEntry.OutputBlobs, compactedIndexBlob)

	logEntryBytes, err := json.Marshal(logEntry)
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobID, err := bm.encryptAndWriteBlobNotLocked(ctx, logEntryBytes, compactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	formatLog(ctx).Debugf("compacted indexes %v into %v and wrote log %v in %v", logEntry.InputBlobs, logEntry.OutputBlobs, compactionLogBlobID, time.Since(t0)) // allow:no-inject-time

	if err := bm.deleteOldIndexBlobs(ctx, compactionLogBlobID); err != nil {
		return errors.Wrap(err, "error deleting old index blobs")
	}

	return nil
}

func (bm *lockFreeManager) getCompactionLogEntries(ctx context.Context, blobs []blob.Metadata) (map[blob.ID]*compactionLogEntry, error) {
	results := map[blob.ID]*compactionLogEntry{}

	for _, cb := range blobs {
		data, err := bm.getIndexBlobInternal(ctx, cb.BlobID)
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

func (bm *Manager) deleteOldIndexBlobs(ctx context.Context, latestBlobID blob.ID) error {
	allCompactionLogBlobs, err := blob.ListAllBlobs(ctx, bm.st, compactionLogBlobPrefix)
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

	entries, err := bm.getCompactionLogEntries(ctx, fetchCompactionLogs)
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

		if err := bm.st.DeleteBlob(ctx, cb); err != nil && err != blob.ErrBlobNotFound {
			formatLog(ctx).Warningf("unable to delete compacted blob %v, %v", cb, err)
		}
	}

	for _, cb := range fetchCompactionLogs {
		log(ctx).Debugf("deleting compaction log blob %v", cb)

		if err := bm.st.DeleteBlob(ctx, cb.BlobID); err != nil && err != blob.ErrBlobNotFound {
			formatLog(ctx).Warningf("unable to delete compaction log blob %v, %v", cb.BlobID, err)
		}
	}

	return nil
}

func blobsOlderThan(bm []blob.Metadata, cutoffTime time.Time) []blob.Metadata {
	var res []blob.Metadata

	for _, m := range bm {
		if m.Timestamp.Before(cutoffTime) {
			res = append(res, m)
		}
	}

	return res
}

func blobTime(bm []blob.Metadata, blobID blob.ID) time.Time {
	for _, m := range bm {
		if m.BlobID == blobID {
			return m.Timestamp
		}
	}

	return time.Time{}
}

func (bm *Manager) addIndexBlobsToBuilder(ctx context.Context, bld packIndexBuilder, indexBlob IndexBlobInfo, opt CompactOptions) error {
	data, err := bm.getIndexBlobInternal(ctx, indexBlob.BlobID)
	if err != nil {
		return errors.Wrapf(err, "error getting index %q", indexBlob.BlobID)
	}

	index, err := openPackIndex(bytes.NewReader(data))
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlob)
	}

	_ = index.Iterate(AllIDs, func(i Info) error {
		if i.Deleted && !opt.DropDeletedBefore.IsZero() && i.Timestamp().Before(opt.DropDeletedBefore) {
			log(ctx).Debugf("skipping content %v deleted at %v", i.ID, i.Timestamp())
			return nil
		}
		bld.Add(i)
		return nil
	})

	return nil
}

func (bm *lockFreeManager) listEffectiveIndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error) {
	compactionLogMetadata, err := blob.ListAllBlobs(ctx, bm.st, compactionLogBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing compaction log entries")
	}

	compactionLogMetadata, err = bm.ownWritesCache.merge(ctx, compactionLogBlobPrefix, compactionLogMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error merging local writes for compaction log entries")
	}

	storageIndexBlobs, err := blob.ListAllBlobs(ctx, bm.st, indexBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing index blobs")
	}

	storageIndexBlobs, err = bm.ownWritesCache.merge(ctx, indexBlobPrefix, storageIndexBlobs)
	if err != nil {
		return nil, errors.Wrap(err, "error merging local writes for index blobs")
	}

	indexMap := map[blob.ID]*IndexBlobInfo{}
	addBlobsToIndex(indexMap, storageIndexBlobs)

	compactionLogs, err := bm.getCompactionLogEntries(ctx, compactionLogMetadata)
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

func addBlobsToIndex(ndx map[blob.ID]*IndexBlobInfo, blobs []blob.Metadata) {
	for _, it := range blobs {
		if ndx[it.BlobID] == nil {
			ndx[it.BlobID] = &IndexBlobInfo{
				BlobID:    it.BlobID,
				Length:    it.Length,
				Timestamp: it.Timestamp,
			}
		}
	}
}

func removeCompactedIndexes(ctx context.Context, m map[blob.ID]*IndexBlobInfo, compactionLogs map[blob.ID]*compactionLogEntry, markAsSuperseded bool) {
	var validCompactionLogs []*compactionLogEntry

	for _, cl := range compactionLogs {
		// only process compaction logs for which we have found all the outputs.
		haveAllOutputs := true

		for _, o := range cl.OutputBlobs {
			if m[o] == nil {
				haveAllOutputs = false

				log(ctx).Debugf("blob %v referenced by compaction log is not found", o)

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
			if md := m[ib]; md != nil && md.Superseded == nil {
				log(ctx).Debugf("ignoring index blob %v (%v) because it's been compacted to %v", ib, md.Timestamp, cl.OutputBlobs)

				if markAsSuperseded {
					md.Superseded = cl.OutputBlobs
				} else {
					delete(m, ib)
				}
			}
		}
	}
}
