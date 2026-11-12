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
	// It is also the size of the output shards, which bounds the memory needed to read them in the next merge.
	DefaultMergeMaxEntriesPerPartition = 500_000

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
	if err := opt.normalize(); err != nil {
		return nil, err
	}

	ranges, watermark, err := m.rangeChainToMerge(ctx)
	if err != nil {
		return nil, err
	}

	stats, rangeBlobs, rangeBlobList := newMergeRangeStats(ranges, watermark, opt)

	opt.Progress("merging %v range checkpoints (epochs %v..%v, %v blobs, %v bytes) in %v partition(s), deletion watermark %v",
		len(ranges), stats.MinEpoch, stats.MaxEpoch, len(rangeBlobList), stats.InputBytes, stats.Partitions, watermark.UTC().Format(time.RFC3339))

	// Index blobs of the epochs after the range checkpoints. Entries there that are not newer than the
	// watermark could lose against a dropped deleted entry today, so those deleted entries must be kept.
	laterBlobs, err := m.laterIndexBlobs(ctx, rangeBlobs)
	if err != nil {
		return nil, err
	}

	spoolDir, err := os.MkdirTemp(opt.TempDir, "kopia-merge-ranges-")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary directory")
	}

	defer os.RemoveAll(spoolDir) //nolint:errcheck

	sp, laterSp, err := m.newMergeSpools(ctx, spoolDir, stats.Partitions, opt.MaxEntriesPerPartition)
	if err != nil {
		return nil, err
	}

	if stats.LaterOldEntries, err = m.spoolEntriesNotNewerThan(ctx, laterSp, laterBlobs, watermark); err != nil {
		return nil, err
	}

	stats.LaterBlobs = len(laterBlobs)

	opt.Progress("read %v later index blobs, %v entries not newer than the watermark", len(laterBlobs), stats.LaterOldEntries)

	if err := m.spoolByPartition(ctx, sp, rangeBlobList, stats, opt.Progress); err != nil {
		return nil, err
	}

	shardFiles, err := mergePartitions(sp, laterSp, stats, watermark, opt)
	if err != nil {
		return nil, err
	}

	if opt.DryRun {
		opt.Progress("dry run: would write %v shard(s), %v entries, %v bytes", stats.OutputShards, stats.OutputEntries, stats.OutputBytes)
		return stats, nil
	}

	if err := m.ensureIndexUnchanged(ctx, stats.MaxEpoch, rangeBlobs, laterBlobs, watermark); err != nil {
		return nil, err
	}

	outputPrefix := epoch.RangeCheckpointBlobPrefix(stats.MinEpoch, stats.MaxEpoch)

	if stats.OutputBlobs, err = m.writeShardFiles(ctx, shardFiles, outputPrefix); err != nil {
		return nil, err
	}

	opt.Progress("wrote %v blob(s) with prefix %v", len(stats.OutputBlobs), outputPrefix)

	m.epochMgr.Invalidate()

	return stats, nil
}

func (opt *MergeRangeOptions) normalize() error {
	if opt.Progress == nil {
		opt.Progress = func(string, ...any) {}
	}

	if opt.MaxEntriesPerPartition == 0 {
		opt.MaxEntriesPerPartition = DefaultMergeMaxEntriesPerPartition
	}

	if opt.Partitions < 0 || opt.Partitions > index.MaxPartitions || opt.MaxEntriesPerPartition < 1 {
		return errors.Errorf("invalid merge options: %v partitions, %v entries per partition", opt.Partitions, opt.MaxEntriesPerPartition)
	}

	return nil
}

// rangeChainToMerge returns the current range checkpoint chain and deletion watermark, if the chain can be merged.
func (m *ManagerV1) rangeChainToMerge(ctx context.Context) ([]*epoch.RangeMetadata, time.Time, error) {
	if err := m.epochMgr.Refresh(ctx); err != nil {
		return nil, time.Time{}, errors.Wrap(err, "refresh")
	}

	cs, err := m.epochMgr.Current(ctx)
	if err != nil {
		return nil, time.Time{}, errors.Wrap(err, "current epoch state")
	}

	ranges := cs.LongestRangeCheckpointSets
	if len(ranges) < 2 { //nolint:mnd
		return nil, time.Time{}, errors.Errorf("nothing to merge: the longest range checkpoint chain has %v element(s)", len(ranges))
	}

	if ranges[0].MinEpoch != epoch.FirstEpoch {
		return nil, time.Time{}, errors.Errorf("range checkpoint chain does not start at epoch %v", epoch.FirstEpoch)
	}

	return ranges, cs.DeletionWatermark, nil
}

// newMergeRangeStats returns the initial statistics and the blobs of the range checkpoint chain.
func newMergeRangeStats(ranges []*epoch.RangeMetadata, watermark time.Time, opt MergeRangeOptions) (*MergeRangeStats, map[blob.ID]blob.Metadata, []blob.Metadata) {
	stats := &MergeRangeStats{
		MinEpoch:          ranges[0].MinEpoch,
		MaxEpoch:          ranges[len(ranges)-1].MaxEpoch,
		InputRanges:       len(ranges),
		DeletionWatermark: watermark,
	}

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

	return stats, rangeBlobs, rangeBlobList
}

// newMergeSpools returns the spools for the range checkpoint entries and for the later entries not newer than the watermark.
func (m *ManagerV1) newMergeSpools(ctx context.Context, dir string, partitions, flushAt int) (rangeSpool, laterSpool *mergeSpool, err error) {
	mp, err := m.formattingOptions.GetMutableParameters(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "mutable parameters")
	}

	newSpool := func(name string) *mergeSpool {
		return &mergeSpool{
			dir:          dir,
			name:         name,
			partitions:   partitions,
			indexVersion: mp.IndexVersion,
			overhead:     m.enc.crypter.Encryptor().Overhead,
			flushAt:      flushAt,
		}
	}

	return newSpool("range"), newSpool("later"), nil
}

// spoolEntriesNotNewerThan spools the entries of the given index blobs that are not newer than the watermark
// and returns their number.
func (m *ManagerV1) spoolEntriesNotNewerThan(ctx context.Context, sp *mergeSpool, blobs []blob.Metadata, watermark time.Time) (int, error) {
	count := 0

	for _, bm := range blobs {
		var addErr error

		if err := m.iterateIndexBlob(ctx, bm.BlobID, func(i index.Info) {
			if addErr != nil || i.Timestamp().After(watermark) {
				return
			}

			count++
			addErr = sp.add(i)
		}); err != nil {
			return 0, errors.Wrapf(err, "error reading later index blob %v", bm.BlobID)
		}

		if addErr != nil {
			return 0, addErr
		}
	}

	return count, sp.flush()
}

// iterateIndexBlob calls cb for every entry of the index blob.
// Only one decrypted copy of the blob is held while iterating, which matters for large range checkpoints.
func (m *ManagerV1) iterateIndexBlob(ctx context.Context, blobID blob.ID, cb func(index.Info)) error {
	data, err := m.decryptedIndexBlob(ctx, blobID)
	if err != nil {
		return err
	}

	ndx, err := index.Open(data, nil, m.enc.crypter.Encryptor().Overhead)
	if err != nil {
		return errors.Wrap(err, "unable to open index blob")
	}

	return errors.Wrap(ndx.Iterate(index.AllIDs, func(i index.Info) error {
		cb(i)
		return nil
	}), "error iterating index blob")
}

// decryptedIndexBlob returns the decrypted index blob as one slice and releases the chunked buffer it was read into.
func (m *ManagerV1) decryptedIndexBlob(ctx context.Context, blobID blob.ID) ([]byte, error) {
	var buf gather.WriteBuffer
	defer buf.Close()

	if err := m.enc.GetEncryptedBlob(ctx, blobID, &buf); err != nil {
		return nil, errors.Wrap(err, "error getting index blob")
	}

	return buf.ToByteSlice(), nil
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
	name         string
	partitions   int
	indexVersion int
	overhead     func() int
	flushAt      int

	pending      []index.Builder
	pendingCount int
}

func (s *mergeSpool) partitionFile(partition int) string {
	return filepath.Join(s.dir, fmt.Sprintf("%v%05d.spool", s.name, partition))
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

	f, err := os.OpenFile(s.partitionFile(partition), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600) //nolint:mnd
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

// readPartition returns the newest spooled entry of every content in the partition and removes the spool file.
func (s *mergeSpool) readPartition(partition int) (index.Builder, error) {
	result := index.Builder{}

	if err := s.iteratePartition(partition, result.Add); err != nil {
		return nil, err
	}

	os.Remove(s.partitionFile(partition)) //nolint:errcheck

	return result, nil
}

// iteratePartition calls cb for every spooled entry of the partition.
func (s *mergeSpool) iteratePartition(partition int, cb func(index.Info)) error {
	f, err := os.Open(s.partitionFile(partition))
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

		if err := m.iterateIndexBlob(ctx, bm.BlobID, func(i index.Info) {
			stats.InputEntries++

			if addErr == nil {
				addErr = sp.add(i)
			}
		}); err != nil {
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
func mergePartitions(sp, laterSp *mergeSpool, stats *MergeRangeStats, watermark time.Time, opt MergeRangeOptions) ([]string, error) {
	out := &mergeOutput{dir: sp.dir, indexVersion: sp.indexVersion, stats: stats, dryRun: opt.DryRun, entries: index.Builder{}}

	for partition := range sp.partitions {
		partitionBuilder, err := sp.readPartition(partition)
		if err != nil {
			return nil, err
		}

		// a content always maps to the same partition, so this holds every later entry the guard needs
		laterOld, err := laterSp.readPartition(partition)
		if err != nil {
			return nil, err
		}

		for _, winner := range partitionBuilder {
			stats.UniqueContents++

			if keepMergedEntry(winner, laterOld, watermark, stats) {
				out.entries.Add(winner)

				stats.OutputEntries++
			}
		}

		if len(out.entries) >= opt.MaxEntriesPerPartition {
			if err := out.flush(); err != nil {
				return nil, err
			}
		}

		if (partition+1)%16 == 0 || partition+1 == sp.partitions {
			opt.Progress("partition %v/%v done: %v unique contents so far, %v dropped, %v kept",
				partition+1, sp.partitions, stats.UniqueContents, stats.DroppedDeleted, stats.OutputEntries)
		}
	}

	return out.finish()
}

// keepMergedEntry decides whether the winning entry of a content goes into the merged checkpoint.
func keepMergedEntry(winner index.Info, laterOld index.Builder, watermark time.Time, stats *MergeRangeStats) bool {
	if !winner.Deleted {
		return true
	}

	if winner.Timestamp().After(watermark) {
		stats.KeptDeletedRecent++
		return true
	}

	// an older entry in a later epoch would become visible if the deleted entry was dropped
	if laterEntry, ok := laterOld[winner.ContentID]; ok && !index.InfoGreaterThan(&laterEntry, &winner) {
		stats.KeptDeletedGuard++
		return true
	}

	stats.DroppedDeleted++

	return false
}

// mergeOutput collects the merged entries and writes them into encoded index shard files.
type mergeOutput struct {
	dir          string
	indexVersion int
	stats        *MergeRangeStats
	dryRun       bool

	entries    index.Builder
	shardFiles []string
}

func (o *mergeOutput) flush() error {
	if len(o.entries) == 0 {
		return nil
	}

	shards, cleanup, err := o.entries.BuildShards(o.indexVersion, true, DefaultIndexShardSize)
	if err != nil {
		return errors.Wrap(err, "unable to build index shards")
	}

	defer cleanup()

	for _, shard := range shards {
		if err := o.writeShard(shard); err != nil {
			return err
		}
	}

	o.entries = index.Builder{}

	return nil
}

func (o *mergeOutput) writeShard(data gather.Bytes) error {
	o.stats.OutputBytes += int64(data.Length())
	o.stats.OutputShards++

	if o.dryRun {
		return nil
	}

	fileName := filepath.Join(o.dir, fmt.Sprintf("out%05d.idx", len(o.shardFiles)))

	if err := writeFile(fileName, data); err != nil {
		return err
	}

	o.shardFiles = append(o.shardFiles, fileName)

	return nil
}

// finish writes the remaining entries and returns the shard files.
func (o *mergeOutput) finish() ([]string, error) {
	if err := o.flush(); err != nil {
		return nil, err
	}

	if o.stats.OutputShards == 0 {
		// everything was dropped: an empty checkpoint still has to replace the chain
		var buf gather.WriteBuffer
		defer buf.Close()

		if err := (index.Builder{}).Build(&buf, o.indexVersion); err != nil {
			return nil, errors.Wrap(err, "unable to build empty index")
		}

		if err := o.writeShard(buf.Bytes()); err != nil {
			return nil, err
		}
	}

	return o.shardFiles, nil
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

		if err := m.iterateIndexBlob(ctx, bm.BlobID, func(i index.Info) {
			if !i.Timestamp().After(watermark) {
				oldEntries++
			}
		}); err != nil {
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
