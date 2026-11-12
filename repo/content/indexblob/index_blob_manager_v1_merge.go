package indexblob

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

// MergeRangeOptions controls MergeRangeCheckpoints.
type MergeRangeOptions struct {
	// Partitions is the number of passes over the input; more passes need less memory.
	Partitions int
	// DryRun computes the statistics without writing anything.
	DryRun bool
	// TempDir is where the decrypted input is stored during the merge (empty: system temp directory).
	TempDir string
	// Progress receives human-readable progress messages (may be nil).
	Progress func(format string, args ...any)
}

// MergeRangeStats describes the result of MergeRangeCheckpoints.
type MergeRangeStats struct {
	MinEpoch          int
	MaxEpoch          int
	InputRanges       int
	InputBlobs        int
	InputBytes        int64
	InputEntries      int64
	UniqueContents    int64
	DroppedDeleted    int64
	KeptDeletedRecent int64
	KeptDeletedGuard  int64
	OutputEntries     int64
	OutputBytes       int64
	OutputBlobs       []blob.ID
	LaterBlobs        int
	LaterOldEntries   int
	DeletionWatermark time.Time
}

// MergeRangeCheckpoints merges the longest chain of range checkpoints into a single range checkpoint
// covering the same epochs, dropping deleted entries that readers already ignore because of the deletion watermark.
func (m *ManagerV1) MergeRangeCheckpoints(ctx context.Context, opt MergeRangeOptions) (*MergeRangeStats, error) {
	progress := opt.Progress
	if progress == nil {
		progress = func(string, ...any) {}
	}

	if opt.Partitions < 1 || opt.Partitions > 256 {
		return nil, errors.Errorf("invalid number of partitions: %v", opt.Partitions)
	}

	if err := m.epochMgr.Refresh(ctx); err != nil {
		return nil, errors.Wrap(err, "refresh")
	}

	cs, err := m.epochMgr.Current(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "current epoch state")
	}

	ranges := cs.LongestRangeCheckpointSets
	if len(ranges) < 2 { //nolint:mnd
		return nil, errors.Errorf("nothing to merge: the longest range checkpoint chain has %v element(s)", len(ranges))
	}

	if ranges[0].MinEpoch != epoch.FirstEpoch {
		return nil, errors.Errorf("range checkpoint chain does not start at epoch %v", epoch.FirstEpoch)
	}

	stats := &MergeRangeStats{
		MinEpoch:          ranges[0].MinEpoch,
		MaxEpoch:          ranges[len(ranges)-1].MaxEpoch,
		InputRanges:       len(ranges),
		DeletionWatermark: cs.DeletionWatermark,
	}

	watermark := cs.DeletionWatermark

	rangeBlobs := map[blob.ID]blob.Metadata{}

	var rangeBlobList []blob.Metadata

	for _, r := range ranges {
		for _, bm := range r.Blobs {
			rangeBlobs[bm.BlobID] = bm
			rangeBlobList = append(rangeBlobList, bm)
			stats.InputBytes += bm.Length
		}
	}

	stats.InputBlobs = len(rangeBlobList)

	progress("merging %v range checkpoints (epochs %v..%v, %v blobs, %v bytes), deletion watermark %v",
		len(ranges), stats.MinEpoch, stats.MaxEpoch, len(rangeBlobList), stats.InputBytes, watermark.UTC().Format(time.RFC3339))

	// Index blobs of the epochs after the range checkpoints. Entries there that are not newer than the
	// watermark could lose against a dropped deleted entry today, so those deleted entries must be kept.
	laterBlobs, err := m.laterIndexBlobs(ctx, rangeBlobs)
	if err != nil {
		return nil, err
	}

	stats.LaterBlobs = len(laterBlobs)

	laterOld := index.Builder{}

	for _, bm := range laterBlobs {
		if err := addIndexBlobsToBuilder(ctx, m.enc, func(i index.Info) {
			if !i.Timestamp().After(watermark) {
				laterOld.Add(i)
			}
		}, bm.BlobID); err != nil {
			return nil, errors.Wrapf(err, "error reading later index blob %v", bm.BlobID)
		}
	}

	stats.LaterOldEntries = len(laterOld)

	progress("read %v later index blobs, %v entries not newer than the watermark", len(laterBlobs), len(laterOld))

	spoolDir, err := os.MkdirTemp(opt.TempDir, "kopia-merge-ranges-")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary directory")
	}

	defer os.RemoveAll(spoolDir) //nolint:errcheck

	inputFiles, err := m.spoolIndexBlobs(ctx, rangeBlobList, spoolDir, progress)
	if err != nil {
		return nil, err
	}

	output := index.Builder{}

	for partition := range opt.Partitions {
		partitionBuilder := index.Builder{}

		for _, inputFile := range inputFiles {
			if err := iterateIndexFile(inputFile, m.enc.crypter.Encryptor().Overhead, func(i index.Info) {
				if i.ContentID.Partition(opt.Partitions) == partition {
					stats.InputEntries++

					partitionBuilder.Add(i)
				}
			}); err != nil {
				return nil, err
			}
		}

		for contentID, winner := range partitionBuilder {
			stats.UniqueContents++

			if winner.Deleted {
				if winner.Timestamp().After(watermark) {
					stats.KeptDeletedRecent++
				} else if laterEntry, ok := laterOld[contentID]; ok && !index.InfoGreaterThan(&laterEntry, &winner) {
					stats.KeptDeletedGuard++
				} else {
					stats.DroppedDeleted++
					continue
				}
			}

			output.Add(winner)
		}

		progress("partition %v/%v done: %v unique contents so far, %v dropped, %v kept",
			partition+1, opt.Partitions, stats.UniqueContents, stats.DroppedDeleted, len(output))
	}

	stats.OutputEntries = int64(len(output))

	mp, err := m.formattingOptions.GetMutableParameters(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "mutable parameters")
	}

	dataShards, cleanupShards, err := output.BuildShards(mp.IndexVersion, true, DefaultIndexShardSize)
	if err != nil {
		return nil, errors.Wrap(err, "unable to build index shards")
	}

	defer cleanupShards()

	for _, ds := range dataShards {
		stats.OutputBytes += int64(ds.Length())
	}

	if opt.DryRun {
		progress("dry run: would write %v shard(s), %v entries, %v bytes", len(dataShards), stats.OutputEntries, stats.OutputBytes)
		return stats, nil
	}

	if err := m.ensureIndexUnchanged(ctx, stats.MaxEpoch, rangeBlobs, laterBlobs, watermark); err != nil {
		return nil, err
	}

	written, err := m.writeShards(ctx, dataShards, epoch.RangeCheckpointBlobPrefix(stats.MinEpoch, stats.MaxEpoch))
	if err != nil {
		return nil, err
	}

	stats.OutputBlobs = written

	progress("wrote %v blob(s) with prefix %v", len(written), epoch.RangeCheckpointBlobPrefix(stats.MinEpoch, stats.MaxEpoch))

	m.epochMgr.Invalidate()

	return stats, nil
}

// laterIndexBlobs returns the active index blobs that are not part of the given range checkpoint blobs.
func (m *ManagerV1) laterIndexBlobs(ctx context.Context, rangeBlobs map[blob.ID]blob.Metadata) ([]blob.Metadata, error) {
	active, _, err := m.epochMgr.GetCompleteIndexSet(ctx, epoch.LatestEpoch)
	if err != nil {
		return nil, errors.Wrap(err, "error getting complete index set")
	}

	var later []blob.Metadata

	foundRangeBlobs := 0

	for _, bm := range active {
		if _, ok := rangeBlobs[bm.BlobID]; ok {
			foundRangeBlobs++
			continue
		}

		later = append(later, bm)
	}

	if foundRangeBlobs != len(rangeBlobs) {
		return nil, errors.Errorf("index state changed: complete index set contains %v of %v range checkpoint blobs", foundRangeBlobs, len(rangeBlobs))
	}

	return later, nil
}

// spoolIndexBlobs decrypts the index blobs once into local files, so that each pass only needs one blob in memory.
func (m *ManagerV1) spoolIndexBlobs(ctx context.Context, blobs []blob.Metadata, spoolDir string, progress func(string, ...any)) ([]string, error) {
	result := make([]string, 0, len(blobs))

	for n, bm := range blobs {
		var data gather.WriteBuffer

		if err := m.enc.GetEncryptedBlob(ctx, bm.BlobID, &data); err != nil {
			data.Close()
			return nil, errors.Wrapf(err, "error getting index %v", bm.BlobID)
		}

		fileName := filepath.Join(spoolDir, fmt.Sprintf("%05d.idx", n))

		f, err := os.Create(fileName) //nolint:gosec
		if err != nil {
			data.Close()
			return nil, errors.Wrap(err, "unable to create spool file")
		}

		_, writeErr := data.Bytes().WriteTo(f)
		closeErr := f.Close()

		data.Close()

		if writeErr != nil || closeErr != nil {
			return nil, errors.Errorf("unable to write spool file %v: %v %v", fileName, writeErr, closeErr)
		}

		// make sure the decrypted blob is a valid index before relying on it
		if err := iterateIndexFile(fileName, m.enc.crypter.Encryptor().Overhead, func(index.Info) {}); err != nil {
			return nil, errors.Wrapf(err, "invalid index blob %v", bm.BlobID)
		}

		result = append(result, fileName)

		if (n+1)%25 == 0 || n+1 == len(blobs) {
			progress("read %v/%v range checkpoint blobs", n+1, len(blobs))
		}
	}

	return result, nil
}

func iterateIndexFile(fileName string, v1PerContentOverhead func() int, cb func(index.Info)) error {
	data, err := os.ReadFile(fileName) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "unable to read spool file")
	}

	ndx, err := index.Open(data, nil, v1PerContentOverhead)
	if err != nil {
		return errors.Wrapf(err, "unable to open index %v", fileName)
	}

	return errors.Wrap(ndx.Iterate(index.AllIDs, func(i index.Info) error {
		cb(i)
		return nil
	}), "error iterating index")
}

// ensureIndexUnchanged fails if a new range checkpoint appeared or if index blobs written during the merge
// contain entries that are not newer than the watermark (those could be shadowed by a dropped entry).
func (m *ManagerV1) ensureIndexUnchanged(ctx context.Context, maxEpoch int, rangeBlobs map[blob.ID]blob.Metadata, laterBlobs []blob.Metadata, watermark time.Time) error {
	if err := m.epochMgr.Refresh(ctx); err != nil {
		return errors.Wrap(err, "refresh")
	}

	cs, err := m.epochMgr.Current(ctx)
	if err != nil {
		return errors.Wrap(err, "current epoch state")
	}

	if n := len(cs.LongestRangeCheckpointSets); n == 0 || cs.LongestRangeCheckpointSets[n-1].MaxEpoch != maxEpoch {
		return errors.New("index state changed: range checkpoints changed during the merge, run again")
	}

	later, err := m.laterIndexBlobs(ctx, rangeBlobs)
	if err != nil {
		return err
	}

	known := map[blob.ID]bool{}
	for _, bm := range laterBlobs {
		known[bm.BlobID] = true
	}

	for _, bm := range later {
		if known[bm.BlobID] {
			continue
		}

		oldEntries := 0

		if err := addIndexBlobsToBuilder(ctx, m.enc, func(i index.Info) {
			if !i.Timestamp().After(watermark) {
				oldEntries++
			}
		}, bm.BlobID); err != nil {
			return errors.Wrapf(err, "error reading new index blob %v", bm.BlobID)
		}

		if oldEntries > 0 {
			return errors.Errorf("index state changed: new index blob %v has %v entries not newer than the watermark, run again", bm.BlobID, oldEntries)
		}
	}

	return nil
}

func (m *ManagerV1) writeShards(ctx context.Context, dataShards []gather.Bytes, outputPrefix blob.ID) ([]blob.ID, error) {
	var rnd [8]byte

	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, errors.Wrap(err, "error getting random session ID")
	}

	// the -c<count> suffix lets readers ignore the set until all shards are written
	sessionID := fmt.Sprintf("s%x-c%v", rnd[:], len(dataShards))

	var (
		encrypted gather.WriteBuffer
		written   []blob.ID
	)

	defer encrypted.Close()

	for _, data := range dataShards {
		encrypted.Reset()

		blobID, err := blobcrypto.Encrypt(m.enc.crypter, data, outputPrefix, blob.ID(sessionID), &encrypted)
		if err != nil {
			return written, errors.Wrap(err, "error encrypting")
		}

		if err := m.st.PutBlob(ctx, blobID, encrypted.Bytes(), blob.PutOptions{}); err != nil {
			return written, errors.Wrap(err, "error writing index blob")
		}

		written = append(written, blobID)
	}

	return written, nil
}
