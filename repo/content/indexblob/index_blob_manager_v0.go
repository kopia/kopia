package indexblob

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/contentparam"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// V0IndexBlobPrefix is the prefix for all legacy (v0) index blobs.
const V0IndexBlobPrefix = "n"

// V0CompactionLogBlobPrefix is the prefix for all legacy (v0) index compactions blobs.
const V0CompactionLogBlobPrefix = "m"

// V0CleanupBlobPrefix is the prefix for all legacy (v0) index cleanup blobs.
const V0CleanupBlobPrefix = "l"

const defaultEventualConsistencySettleTime = 1 * time.Hour

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

// IndexFormattingOptions provides options for formatting index blobs.
type IndexFormattingOptions interface {
	GetMutableParameters(ctx context.Context) (format.MutableParameters, error)
}

// ManagerV0 is a V0 (legacy) implementation of index blob manager.
type ManagerV0 struct {
	st                blob.Storage
	enc               *EncryptionManager
	timeNow           func() time.Time
	formattingOptions IndexFormattingOptions
	log               *contentlog.Logger
}

// ListIndexBlobInfos lists active blob info structs.
func (m *ManagerV0) ListIndexBlobInfos(ctx context.Context) ([]Metadata, error) {
	activeIndexBlobs, _, err := m.ListActiveIndexBlobs(ctx)
	if err != nil {
		return nil, err
	}

	q := make([]Metadata, 0, len(activeIndexBlobs))

	for _, activeIndexBlob := range activeIndexBlobs {
		// skip the V0 blob poison that is used to prevent client reads.
		if activeIndexBlob.BlobID == format.LegacyIndexPoisonBlobID {
			continue
		}

		q = append(q, activeIndexBlob)
	}

	return q, nil
}

// ListActiveIndexBlobs lists the metadata for active index blobs and returns the cut-off time
// before which all deleted index entries should be treated as non-existent.
func (m *ManagerV0) ListActiveIndexBlobs(ctx context.Context) ([]Metadata, time.Time, error) {
	var compactionLogMetadata, storageIndexBlobs []blob.Metadata

	var eg errgroup.Group

	// list index and cleanup blobs in parallel.
	eg.Go(func() error {
		v, err := blob.ListAllBlobs(ctx, m.st, V0CompactionLogBlobPrefix)
		compactionLogMetadata = v

		return errors.Wrap(err, "error listing compaction blobs")
	})

	eg.Go(func() error {
		v, err := blob.ListAllBlobs(ctx, m.st, V0IndexBlobPrefix)
		storageIndexBlobs = v

		return errors.Wrap(err, "error listing index blobs")
	})

	if err := eg.Wait(); err != nil {
		return nil, time.Time{}, errors.Wrap(err, "error listing indexes")
	}

	contentlog.Log2(ctx, m.log, "found index blobs",
		blobparam.BlobMetadataList("storageIndexBlobs", storageIndexBlobs),
		blobparam.BlobMetadataList("compactionLogMetadata", compactionLogMetadata))

	indexMap := map[blob.ID]*Metadata{}
	addBlobsToIndex(indexMap, storageIndexBlobs)

	compactionLogs, err := m.getCompactionLogEntries(ctx, compactionLogMetadata)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "error reading compaction log")
	}

	// remove entries from indexMap that have been compacted and replaced by other indexes.
	m.removeCompactedIndexes(ctx, indexMap, compactionLogs)

	var results []Metadata
	for _, v := range indexMap {
		results = append(results, *v)
	}

	contentlog.Log1(ctx, m.log,
		"active index blobs",
		MetadataListParam("results", results))

	return results, time.Time{}, nil
}

// Invalidate invalidates any caches.
func (m *ManagerV0) Invalidate() {
}

// Compact performs compaction of index blobs by merging smaller ones into larger
// and registering compaction and cleanup blobs in the repository.
func (m *ManagerV0) Compact(ctx context.Context, opt CompactOptions) (*maintenancestats.CompactIndexesStats, error) {
	indexBlobs, _, err := m.ListActiveIndexBlobs(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "error listing active index blobs")
	}

	mp, mperr := m.formattingOptions.GetMutableParameters(ctx)
	if mperr != nil {
		return nil, errors.Wrap(mperr, "mutable parameters")
	}

	blobsToCompact := m.getBlobsToCompact(ctx, indexBlobs, opt, mp)

	compacted, err := m.compactIndexBlobs(ctx, blobsToCompact, opt)
	if err != nil {
		return nil, errors.Wrap(err, "error performing compaction")
	}

	if err := m.cleanup(ctx, opt.maxEventualConsistencySettleTime()); err != nil {
		return nil, errors.Wrap(err, "error cleaning up index blobs")
	}

	if compacted {
		return &maintenancestats.CompactIndexesStats{
			DroppedContentsDeletedBefore: opt.DropDeletedBefore,
		}, nil
	}

	return nil, nil
}

func (m *ManagerV0) registerCompaction(ctx context.Context, inputs, outputs []blob.Metadata, maxEventualConsistencySettleTime time.Duration) error {
	logEntryBytes, err := json.Marshal(&compactionLogEntry{
		InputMetadata:  inputs,
		OutputMetadata: outputs,
	})
	if err != nil {
		return errors.Wrap(err, "unable to marshal log entry bytes")
	}

	compactionLogBlobMetadata, err := m.enc.EncryptAndWriteBlob(ctx, gather.FromSlice(logEntryBytes), V0CompactionLogBlobPrefix, "")
	if err != nil {
		return errors.Wrap(err, "unable to write compaction log")
	}

	contentlog.Log4(ctx, m.log,
		"registered compaction",
		blobparam.BlobMetadataList("inputs", inputs),
		blobparam.BlobMetadataList("outputs", outputs),
		blobparam.BlobID("compactionLogBlobID", compactionLogBlobMetadata.BlobID),
		logparam.Time("compactionLogBlobTimestamp", compactionLogBlobMetadata.Timestamp))

	if err := m.deleteOldBlobs(ctx, compactionLogBlobMetadata, maxEventualConsistencySettleTime); err != nil {
		return errors.Wrap(err, "error deleting old index blobs")
	}

	return nil
}

// WriteIndexBlobs writes the provided data shards into new index blobs optionally appending the provided suffix.
func (m *ManagerV0) WriteIndexBlobs(ctx context.Context, dataShards []gather.Bytes, suffix blob.ID) ([]blob.Metadata, error) {
	var result []blob.Metadata

	for _, data := range dataShards {
		bm, err := m.enc.EncryptAndWriteBlob(ctx, data, V0IndexBlobPrefix, suffix)
		if err != nil {
			return nil, errors.Wrap(err, "error writing index blob")
		}

		result = append(result, bm)
	}

	return result, nil
}

func (m *ManagerV0) getCompactionLogEntries(ctx context.Context, blobs []blob.Metadata) (map[blob.ID]*compactionLogEntry, error) {
	results := map[blob.ID]*compactionLogEntry{}

	var data gather.WriteBuffer
	defer data.Close()

	for _, cb := range blobs {
		err := m.enc.GetEncryptedBlob(ctx, cb.BlobID, &data)

		if errors.Is(err, blob.ErrBlobNotFound) {
			continue
		}

		if err != nil {
			return nil, errors.Wrapf(err, "unable to read compaction blob %q", cb.BlobID)
		}

		le := &compactionLogEntry{}

		if err := json.NewDecoder(data.Bytes().Reader()).Decode(le); err != nil {
			return nil, errors.Wrap(err, "unable to read compaction log entry %q")
		}

		le.metadata = cb

		results[cb.BlobID] = le
	}

	return results, nil
}

func (m *ManagerV0) getCleanupEntries(ctx context.Context, latestServerBlobTime time.Time, blobs []blob.Metadata) (map[blob.ID]*cleanupEntry, error) {
	results := map[blob.ID]*cleanupEntry{}

	var data gather.WriteBuffer
	defer data.Close()

	for _, cb := range blobs {
		data.Reset()

		err := m.enc.GetEncryptedBlob(ctx, cb.BlobID, &data)

		if errors.Is(err, blob.ErrBlobNotFound) {
			continue
		}

		if err != nil {
			return nil, errors.Wrapf(err, "unable to read compaction blob %q", cb.BlobID)
		}

		le := &cleanupEntry{}

		if err := json.NewDecoder(data.Bytes().Reader()).Decode(le); err != nil {
			return nil, errors.Wrap(err, "unable to read compaction log entry %q")
		}

		le.age = latestServerBlobTime.Sub(le.CleanupScheduleTime)

		results[cb.BlobID] = le
	}

	return results, nil
}

func (m *ManagerV0) deleteOldBlobs(ctx context.Context, latestBlob blob.Metadata, maxEventualConsistencySettleTime time.Duration) error {
	allCompactionLogBlobs, err := blob.ListAllBlobs(ctx, m.st, V0CompactionLogBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing compaction log blobs")
	}

	// look for server-assigned timestamp of the compaction log entry we just wrote as a reference.
	// we're assuming server-generated timestamps are somewhat reasonable and time is moving
	compactionLogServerTimeCutoff := latestBlob.Timestamp.Add(-maxEventualConsistencySettleTime)
	compactionBlobs := blobsOlderThan(allCompactionLogBlobs, compactionLogServerTimeCutoff)

	contentlog.Log3(ctx, m.log,
		"fetching compaction logs",
		logparam.Int("compactionBlobs", len(compactionBlobs)),
		logparam.Int("allCompactionLogBlobs", len(allCompactionLogBlobs)),
		logparam.Time("compactionLogServerTimeCutoff", compactionLogServerTimeCutoff))

	compactionBlobEntries, err := m.getCompactionLogEntries(ctx, compactionBlobs)
	if err != nil {
		return errors.Wrap(err, "unable to get compaction log entries")
	}

	indexBlobsToDelete := m.findIndexBlobsToDelete(ctx, latestBlob.Timestamp, compactionBlobEntries, maxEventualConsistencySettleTime)

	// note that we must always delete index blobs first before compaction logs
	// otherwise we may inadvertently resurrect an index blob that should have been removed.
	if err := m.deleteBlobsFromStorageAndCache(ctx, indexBlobsToDelete); err != nil {
		return errors.Wrap(err, "unable to delete compaction logs")
	}

	compactionLogBlobsToDelayCleanup := m.findCompactionLogBlobsToDelayCleanup(ctx, compactionBlobs)

	if err := m.delayCleanupBlobs(ctx, compactionLogBlobsToDelayCleanup, latestBlob.Timestamp); err != nil {
		return errors.Wrap(err, "unable to schedule delayed cleanup of blobs")
	}

	return nil
}

func (m *ManagerV0) findIndexBlobsToDelete(ctx context.Context, latestServerBlobTime time.Time, entries map[blob.ID]*compactionLogEntry, maxEventualConsistencySettleTime time.Duration) []blob.ID {
	tmp := map[blob.ID]bool{}

	for _, cl := range entries {
		// are the input index blobs in this compaction eligible for deletion?
		if age := latestServerBlobTime.Sub(cl.metadata.Timestamp); age < maxEventualConsistencySettleTime {
			contentlog.Log3(ctx, m.log,
				"not deleting compacted index blob used as inputs for compaction",
				blobparam.BlobID("blobID", cl.metadata.BlobID),
				logparam.Duration("age", age),
				logparam.Duration("maxEventualConsistencySettleTime", maxEventualConsistencySettleTime))

			continue
		}

		for _, b := range cl.InputMetadata {
			contentlog.Log2(ctx, m.log,
				"will delete old index",
				blobparam.BlobMetadata("b", b),
				blobparam.BlobMetadataList("outputMetadata", cl.OutputMetadata))

			tmp[b.BlobID] = true
		}
	}

	var result []blob.ID

	for k := range tmp {
		result = append(result, k)
	}

	return result
}

func (m *ManagerV0) findCompactionLogBlobsToDelayCleanup(ctx context.Context, compactionBlobs []blob.Metadata) []blob.ID {
	var result []blob.ID

	for _, cb := range compactionBlobs {
		contentlog.Log1(ctx, m.log, "will delete compaction log blob",
			blobparam.BlobMetadata("cb", cb))

		result = append(result, cb.BlobID)
	}

	return result
}

func (m *ManagerV0) findBlobsToDelete(entries map[blob.ID]*cleanupEntry, maxEventualConsistencySettleTime time.Duration) (compactionLogs, cleanupBlobs []blob.ID) {
	for k, e := range entries {
		if e.age >= maxEventualConsistencySettleTime {
			compactionLogs = append(compactionLogs, e.BlobIDs...)
			cleanupBlobs = append(cleanupBlobs, k)
		}
	}

	return compactionLogs, cleanupBlobs
}

func (m *ManagerV0) delayCleanupBlobs(ctx context.Context, blobIDs []blob.ID, cleanupScheduleTime time.Time) error {
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

	if _, err := m.enc.EncryptAndWriteBlob(ctx, gather.FromSlice(payload), V0CleanupBlobPrefix, ""); err != nil {
		return errors.Wrap(err, "unable to cleanup log")
	}

	return nil
}

func (m *ManagerV0) deleteBlobsFromStorageAndCache(ctx context.Context, blobIDs []blob.ID) error {
	for _, blobID := range blobIDs {
		if err := m.st.DeleteBlob(ctx, blobID); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			contentlog.Log2(ctx, m.log, "delete-blob failed", blobparam.BlobID("blobID", blobID), logparam.Error("err", err))
			return errors.Wrapf(err, "unable to delete blob %v", blobID)
		}

		contentlog.Log1(ctx, m.log, "delete-blob succeeded", blobparam.BlobID("blobID", blobID))
	}

	return nil
}

func (m *ManagerV0) cleanup(ctx context.Context, maxEventualConsistencySettleTime time.Duration) error {
	allCleanupBlobs, err := blob.ListAllBlobs(ctx, m.st, V0CleanupBlobPrefix)
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

	if err := m.st.FlushCaches(ctx); err != nil {
		contentlog.Log1(ctx, m.log, "error flushing caches", logparam.Error("err", err))
	}

	return nil
}

func (m *ManagerV0) getBlobsToCompact(ctx context.Context, indexBlobs []Metadata, opt CompactOptions, mp format.MutableParameters) []Metadata {
	var (
		nonCompactedBlobs, verySmallBlobs                                              []Metadata
		totalSizeNonCompactedBlobs, totalSizeVerySmallBlobs, totalSizeMediumSizedBlobs int64
		mediumSizedBlobCount                                                           int
	)

	for _, b := range indexBlobs {
		if b.Length > int64(mp.MaxPackSize) && !opt.AllIndexes {
			continue
		}

		nonCompactedBlobs = append(nonCompactedBlobs, b)
		totalSizeNonCompactedBlobs += b.Length

		if b.Length < int64(mp.MaxPackSize)/verySmallContentFraction {
			verySmallBlobs = append(verySmallBlobs, b)
			totalSizeVerySmallBlobs += b.Length
		} else {
			mediumSizedBlobCount++
			totalSizeMediumSizedBlobs += b.Length
		}
	}

	if len(nonCompactedBlobs) < opt.MaxSmallBlobs {
		// current count is below min allowed - nothing to do
		contentlog.Log(ctx, m.log, "no small contents to Compact")
		return nil
	}

	if len(verySmallBlobs) > len(nonCompactedBlobs)/2 && mediumSizedBlobCount+1 < opt.MaxSmallBlobs {
		contentlog.Log1(ctx, m.log, "compacting very small contents", logparam.Int("len", len(verySmallBlobs)))
		return verySmallBlobs
	}

	contentlog.Log1(ctx, m.log, "compacting all non-compacted contents", logparam.Int("len", len(nonCompactedBlobs)))

	return nonCompactedBlobs
}

func (m *ManagerV0) compactIndexBlobs(ctx context.Context, indexBlobs []Metadata, opt CompactOptions) (bool, error) {
	if len(indexBlobs) <= 1 && opt.DropDeletedBefore.IsZero() && len(opt.DropContents) == 0 {
		return false, nil
	}

	mp, mperr := m.formattingOptions.GetMutableParameters(ctx)
	if mperr != nil {
		return false, errors.Wrap(mperr, "mutable parameters")
	}

	bld := make(index.Builder)

	var inputs, outputs []blob.Metadata

	for _, indexBlob := range indexBlobs {
		contentlog.Log2(ctx, m.log,
			"compacting-entries",
			blobparam.BlobMetadata("indexBlob", indexBlob.Metadata),
			blobparam.BlobMetadataList("superseded", indexBlob.Superseded))

		if err := addIndexBlobsToBuilder(ctx, m.enc, bld.Add, indexBlob.BlobID); err != nil {
			return false, errors.Wrap(err, "error adding index to builder")
		}

		inputs = append(inputs, indexBlob.Metadata)
	}

	// after we built index map in memory, drop contents from it
	// we must do it after all input blobs have been merged, otherwise we may resurrect contents.
	m.dropContentsFromBuilder(ctx, bld, opt)

	dataShards, cleanupShards, err := bld.BuildShards(mp.IndexVersion, false, DefaultIndexShardSize)
	if err != nil {
		return false, errors.Wrap(err, "unable to build an index")
	}

	defer cleanupShards()

	compactedIndexBlobs, err := m.WriteIndexBlobs(ctx, dataShards, "")
	if err != nil {
		return false, errors.Wrap(err, "unable to write compacted indexes")
	}

	outputs = append(outputs, compactedIndexBlobs...)

	if err := m.registerCompaction(ctx, inputs, outputs, opt.maxEventualConsistencySettleTime()); err != nil {
		return false, errors.Wrap(err, "unable to register compaction")
	}

	return true, nil
}

func (m *ManagerV0) dropContentsFromBuilder(ctx context.Context, bld index.Builder, opt CompactOptions) {
	for _, dc := range opt.DropContents {
		if _, ok := bld[dc]; ok {
			contentlog.Log1(ctx, m.log, "manual-drop-from-index", logparam.String("dc", dc.String()))
			delete(bld, dc)
		}
	}

	if !opt.DropDeletedBefore.IsZero() {
		contentlog.Log1(ctx, m.log, "drop-content-deleted-before", logparam.Time("dropDeletedBefore", opt.DropDeletedBefore))

		for _, i := range bld {
			if i.Deleted && i.Timestamp().Before(opt.DropDeletedBefore) {
				contentlog.Log2(ctx, m.log, "drop-from-index-old-deleted",
					contentparam.ContentID("contentID", i.ContentID),
					logparam.Time("timestamp", i.Timestamp()))
				delete(bld, i.ContentID)
			}
		}

		contentlog.Log1(ctx, m.log, "finished drop-content-deleted-before", logparam.Time("dropDeletedBefore", opt.DropDeletedBefore))
	}
}

func addIndexBlobsToBuilder(ctx context.Context, enc *EncryptionManager, addEntry func(index.Info), indexBlobID blob.ID) error {
	var data gather.WriteBuffer
	defer data.Close()

	err := enc.GetEncryptedBlob(ctx, indexBlobID, &data)
	if err != nil {
		return errors.Wrapf(err, "error getting index %q", indexBlobID)
	}

	ndx, err := index.Open(data.ToByteSlice(), nil, enc.crypter.Encryptor().Overhead)
	if err != nil {
		return errors.Wrapf(err, "unable to open index blob %q", indexBlobID)
	}

	_ = ndx.Iterate(index.AllIDs, func(i index.Info) error {
		addEntry(i)
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

func (m *ManagerV0) removeCompactedIndexes(ctx context.Context, bimap map[blob.ID]*Metadata, compactionLogs map[blob.ID]*compactionLogEntry) {
	var validCompactionLogs []*compactionLogEntry

	for _, cl := range compactionLogs {
		// only process compaction logs for which we have found all the outputs.
		haveAllOutputs := true

		for _, o := range cl.OutputMetadata {
			if bimap[o.BlobID] == nil {
				haveAllOutputs = false

				contentlog.Log1(ctx, m.log, "blob referenced by compaction log is not found",
					blobparam.BlobID("blobID", o.BlobID))

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
				contentlog.Log2(ctx, m.log, "ignore-index-blob",
					blobparam.BlobMetadata("ib", ib),
					blobparam.BlobMetadataList("outputMetadata", cl.OutputMetadata))

				delete(bimap, ib.BlobID)
			}
		}
	}
}

// NewManagerV0 creates new instance of ManagerV0 with all required parameters set.
func NewManagerV0(
	st blob.Storage,
	enc *EncryptionManager,
	timeNow func() time.Time,
	formattingOptions IndexFormattingOptions,
	log *contentlog.Logger,
) *ManagerV0 {
	return &ManagerV0{st, enc, timeNow, formattingOptions, log}
}

var _ Manager = (*ManagerV0)(nil)
