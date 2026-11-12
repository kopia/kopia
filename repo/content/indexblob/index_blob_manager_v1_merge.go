package indexblob

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
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

const (
	// DefaultMergeMaxEntriesPerPartition bounds the number of index entries held in memory during a merge.
	DefaultMergeMaxEntriesPerPartition = 1_000_000

	// estimated encoded size of an index entry, used to choose the number of partitions before reading the input.
	estimatedBytesPerIndexEntry = 30

	spoolChunkHeaderSize = 8
)

// MergeRangeOptions controls MergeRangeCheckpoints.
type MergeRangeOptions struct {
	// Partitions is the number of partitions the content IDs are split into (0: derived from the input size).
	Partitions int
	// MaxEntriesPerPartition bounds the number of entries held in memory (0: DefaultMergeMaxEntriesPerPartition).
	MaxEntriesPerPartition int
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
	Partitions        int
	UniqueContents    int64
	DroppedDeleted    int64
	KeptDeletedRecent int64
	KeptDeletedGuard  int64
	OutputEntries     int64
	OutputBytes       int64
	OutputShards      int
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

	if opt.MaxEntriesPerPartition == 0 {
		opt.MaxEntriesPerPartition = DefaultMergeMaxEntriesPerPartition
	}

	if opt.Partitions < 0 || opt.Partitions > index.MaxPartitions || opt.MaxEntriesPerPartition < 1 {
		return nil, errors.Errorf("invalid merge options: %v partitions, %v entries per partition", opt.Partitions, opt.MaxEntriesPerPartition)
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
	stats.Partitions = opt.Partitions

	if stats.Partitions == 0 {
		stats.Partitions = partitionsForInput(stats.InputBytes, opt.MaxEntriesPerPartition)
	}

	progress("merging %v range checkpoints (epochs %v..%v, %v blobs, %v bytes) in %v partition(s), deletion watermark %v",
		len(ranges), stats.MinEpoch, stats.MaxEpoch, len(rangeBlobList), stats.InputBytes, stats.Partitions, watermark.UTC().Format(time.RFC3339))

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

	mp, err := m.formattingOptions.GetMutableParameters(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "mutable parameters")
	}

	spoolDir, err := os.MkdirTemp(opt.TempDir, "kopia-merge-ranges-")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary directory")
	}

	defer os.RemoveAll(spoolDir) //nolint:errcheck

	sp := &mergeSpool{
		dir:          spoolDir,
		partitions:   stats.Partitions,
		indexVersion: mp.IndexVersion,
		overhead:     m.enc.crypter.Encryptor().Overhead,
		flushAt:      opt.MaxEntriesPerPartition,
	}

	if err := m.spoolByPartition(ctx, sp, rangeBlobList, stats, progress); err != nil {
		return nil, err
	}

	shardFiles, err := mergePartitions(sp, stats, laterOld, watermark, opt, progress)
	if err != nil {
		return nil, err
	}

	if opt.DryRun {
		progress("dry run: would write %v shard(s), %v entries, %v bytes", stats.OutputShards, stats.OutputEntries, stats.OutputBytes)
		return stats, nil
	}

	if err := m.ensureIndexUnchanged(ctx, stats.MaxEpoch, rangeBlobs, laterBlobs, watermark); err != nil {
		return nil, err
	}

	written, err := m.writeShardFiles(ctx, shardFiles, epoch.RangeCheckpointBlobPrefix(stats.MinEpoch, stats.MaxEpoch))
	if err != nil {
		return nil, err
	}

	stats.OutputBlobs = written

	progress("wrote %v blob(s) with prefix %v", len(written), epoch.RangeCheckpointBlobPrefix(stats.MinEpoch, stats.MaxEpoch))

	m.epochMgr.Invalidate()

	return stats, nil
}

// partitionsForInput estimates the number of partitions so that each one holds about maxEntriesPerPartition entries.
func partitionsForInput(inputBytes int64, maxEntriesPerPartition int) int {
	estimatedEntries := inputBytes / estimatedBytesPerIndexEntry

	partitions := int(estimatedEntries/int64(maxEntriesPerPartition)) + 1

	return min(partitions, index.MaxPartitions)
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

// mergeSpool stores the input entries in one file per partition, as a sequence of encoded index chunks.
type mergeSpool struct {
	dir          string
	partitions   int
	indexVersion int
	overhead     func() int
	flushAt      int

	pending      []index.Builder
	pendingCount int
}

func (s *mergeSpool) partitionFile(partition int) string {
	return filepath.Join(s.dir, fmt.Sprintf("p%05d.spool", partition))
}

func (s *mergeSpool) add(i index.Info) error {
	if s.pending == nil {
		s.pending = make([]index.Builder, s.partitions)
	}

	p := i.ContentID.Partition(s.partitions)
	if s.pending[p] == nil {
		s.pending[p] = index.Builder{}
	}

	s.pending[p].Add(i)
	s.pendingCount++

	if s.pendingCount >= s.flushAt {
		return s.flush()
	}

	return nil
}

// flush appends the pending entries of every partition to its spool file.
func (s *mergeSpool) flush() error {
	for p, b := range s.pending {
		if len(b) == 0 {
			continue
		}

		if err := s.appendChunk(p, b); err != nil {
			return err
		}

		s.pending[p] = nil
	}

	s.pendingCount = 0

	return nil
}

func (s *mergeSpool) appendChunk(partition int, b index.Builder) error {
	var buf gather.WriteBuffer
	defer buf.Close()

	if err := b.Build(&buf, s.indexVersion); err != nil {
		return errors.Wrap(err, "unable to encode spool chunk")
	}

	f, err := os.OpenFile(s.partitionFile(partition), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec,mnd
	if err != nil {
		return errors.Wrap(err, "unable to open spool file")
	}

	var header [spoolChunkHeaderSize]byte

	binary.BigEndian.PutUint64(header[:], uint64(buf.Length())) //nolint:gosec

	_, err = f.Write(header[:])
	if err == nil {
		_, err = buf.Bytes().WriteTo(f)
	}

	if closeErr := f.Close(); err == nil {
		err = closeErr
	}

	return errors.Wrap(err, "unable to write spool file")
}

// iteratePartition calls cb for every spooled entry of the partition.
func (s *mergeSpool) iteratePartition(partition int, cb func(index.Info)) error {
	f, err := os.Open(s.partitionFile(partition)) //nolint:gosec
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return errors.Wrap(err, "unable to open spool file")
	}

	defer f.Close() //nolint:errcheck

	r := bufio.NewReader(f)

	for {
		var header [spoolChunkHeaderSize]byte

		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return errors.Wrap(err, "unable to read spool chunk header")
		}

		chunk := make([]byte, binary.BigEndian.Uint64(header[:]))

		if _, err := io.ReadFull(r, chunk); err != nil {
			return errors.Wrap(err, "unable to read spool chunk")
		}

		ndx, err := index.Open(chunk, nil, s.overhead)
		if err != nil {
			return errors.Wrap(err, "unable to open spool chunk")
		}

		if err := ndx.Iterate(index.AllIDs, func(i index.Info) error {
			cb(i)
			return nil
		}); err != nil {
			return errors.Wrap(err, "error iterating spool chunk")
		}
	}
}

// spoolByPartition reads each range checkpoint blob once and distributes its entries to the partition files.
func (m *ManagerV1) spoolByPartition(ctx context.Context, sp *mergeSpool, blobs []blob.Metadata, stats *MergeRangeStats, progress func(string, ...any)) error {
	for n, bm := range blobs {
		var addErr error

		if err := addIndexBlobsToBuilder(ctx, m.enc, func(i index.Info) {
			stats.InputEntries++

			if addErr == nil {
				addErr = sp.add(i)
			}
		}, bm.BlobID); err != nil {
			return errors.Wrapf(err, "error reading range checkpoint blob %v", bm.BlobID)
		}

		if addErr != nil {
			return addErr
		}

		if (n+1)%25 == 0 || n+1 == len(blobs) {
			progress("read %v/%v range checkpoint blobs, %v entries", n+1, len(blobs), stats.InputEntries)
		}
	}

	return sp.flush()
}

// mergePartitions picks the winner of every content, drops the deleted ones below the watermark
// and writes the result into encoded index shard files.
func mergePartitions(sp *mergeSpool, stats *MergeRangeStats, laterOld index.Builder, watermark time.Time, opt MergeRangeOptions, progress func(string, ...any)) ([]string, error) {
	var (
		shardFiles []string
		output     = index.Builder{}
	)

	flushOutput := func() error {
		if len(output) == 0 {
			return nil
		}

		shards, cleanup, err := output.BuildShards(sp.indexVersion, true, DefaultIndexShardSize)
		if err != nil {
			return errors.Wrap(err, "unable to build index shards")
		}

		defer cleanup()

		for _, shard := range shards {
			stats.OutputBytes += int64(shard.Length())
			stats.OutputShards++

			if opt.DryRun {
				continue
			}

			fileName := filepath.Join(sp.dir, fmt.Sprintf("out%05d.idx", len(shardFiles)))

			if err := writeFile(fileName, shard); err != nil {
				return err
			}

			shardFiles = append(shardFiles, fileName)
		}

		output = index.Builder{}

		return nil
	}

	for partition := range sp.partitions {
		partitionBuilder := index.Builder{}

		if err := sp.iteratePartition(partition, partitionBuilder.Add); err != nil {
			return nil, err
		}

		os.Remove(sp.partitionFile(partition)) //nolint:errcheck

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
			stats.OutputEntries++
		}

		if len(output) >= opt.MaxEntriesPerPartition {
			if err := flushOutput(); err != nil {
				return nil, err
			}
		}

		if (partition+1)%16 == 0 || partition+1 == sp.partitions {
			progress("partition %v/%v done: %v unique contents so far, %v dropped, %v kept",
				partition+1, sp.partitions, stats.UniqueContents, stats.DroppedDeleted, stats.OutputEntries)
		}
	}

	if err := flushOutput(); err != nil {
		return nil, err
	}

	if stats.OutputShards == 0 {
		// everything was dropped: an empty checkpoint still has to replace the chain
		return writeEmptyShard(sp, stats, opt.DryRun)
	}

	return shardFiles, nil
}

func writeEmptyShard(sp *mergeSpool, stats *MergeRangeStats, dryRun bool) ([]string, error) {
	var buf gather.WriteBuffer
	defer buf.Close()

	if err := (index.Builder{}).Build(&buf, sp.indexVersion); err != nil {
		return nil, errors.Wrap(err, "unable to build empty index")
	}

	stats.OutputBytes += int64(buf.Length())
	stats.OutputShards++

	if dryRun {
		return nil, nil
	}

	fileName := filepath.Join(sp.dir, "out-empty.idx")

	return []string{fileName}, writeFile(fileName, buf.Bytes())
}

func writeFile(fileName string, data gather.Bytes) error {
	f, err := os.Create(fileName) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "unable to create spool file")
	}

	_, err = data.WriteTo(f)

	if closeErr := f.Close(); err == nil {
		err = closeErr
	}

	return errors.Wrap(err, "unable to write spool file")
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

// writeShardFiles encrypts and uploads the shard files as one complete set.
func (m *ManagerV1) writeShardFiles(ctx context.Context, shardFiles []string, outputPrefix blob.ID) ([]blob.ID, error) {
	var rnd [8]byte

	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, errors.Wrap(err, "error getting random session ID")
	}

	// the -c<count> suffix lets readers ignore the set until all shards are written
	sessionID := fmt.Sprintf("s%x-c%v", rnd[:], len(shardFiles))

	var (
		encrypted gather.WriteBuffer
		written   []blob.ID
	)

	defer encrypted.Close()

	for _, fileName := range shardFiles {
		data, err := os.ReadFile(fileName) //nolint:gosec
		if err != nil {
			return written, errors.Wrap(err, "unable to read shard file")
		}

		encrypted.Reset()

		blobID, err := blobcrypto.Encrypt(m.enc.crypter, gather.FromSlice(data), outputPrefix, blob.ID(sessionID), &encrypted)
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
