package indexblob_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
)

const roundsPerEpoch = 21 // the epoch advances after 20 index blobs

type mergeTestEnv struct {
	t     *testing.T
	ctx   context.Context
	env   *repotesting.Environment
	clock *faketime.ClockTimeWithOffset
	rnd   *rand.Rand

	epochCount  int
	epochStart  []time.Time
	idsByEpoch  [][]content.ID
	dataByID    map[content.ID][]byte
	guardID     content.ID
	guardWriter repo.DirectRepositoryWriter
}

func newMergeTestEnv(t *testing.T) *mergeTestEnv {
	t.Helper()

	e := &mergeTestEnv{
		t:        t,
		clock:    faketime.NewClockTimeWithOffset(0),
		rnd:      rand.New(rand.NewSource(42)), //nolint:gosec
		dataByID: map[content.ID][]byte{},
	}

	e.ctx, e.env = repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{OpenOptions: e.openOpt})

	return e
}

func (e *mergeTestEnv) openOpt(o *repo.Options) {
	o.TimeNowFunc = e.clock.NowFunc()
}

func (e *mergeTestEnv) writer() repo.DirectRepositoryWriter {
	return e.env.RepositoryWriter
}

func (e *mergeTestEnv) epochManager() *epoch.Manager {
	em, ok, err := e.writer().ContentManager().EpochManager(e.ctx)
	require.NoError(e.t, err)
	require.True(e.t, ok, "epoch manager must be enabled")

	return em
}

func (e *mergeTestEnv) writeContent(w repo.DirectRepositoryWriter, data []byte) content.ID {
	id, err := w.ContentManager().WriteContent(e.ctx, gather.FromSlice(data), "", content.NoCompression)
	require.NoError(e.t, err)

	e.dataByID[id] = data

	return id
}

// runEpochMaintenance does what the index part of kopia maintenance does.
func (e *mergeTestEnv) runEpochMaintenance() {
	em := e.epochManager()

	require.NoError(e.t, em.Refresh(e.ctx))

	_, err := em.MaybeAdvanceWriteEpoch(e.ctx)
	require.NoError(e.t, err)

	// like kopia maintenance: one single-epoch compaction per run
	require.NoError(e.t, em.Refresh(e.ctx))

	_, err = em.MaybeCompactSingleEpoch(e.ctx)
	require.NoError(e.t, err)

	require.NoError(e.t, em.Refresh(e.ctx))

	_, err = em.MaybeGenerateRangeCheckpoint(e.ctx)
	require.NoError(e.t, err)

	_, err = em.CleanupSupersededIndexes(e.ctx)
	require.NoError(e.t, err)

	em.Flush()
}

// simulateEpoch writes contents, deletes contents from 3 epochs ago and re-creates some contents deleted earlier.
func (e *mergeTestEnv) simulateEpoch() {
	epochIndex := e.epochCount
	e.epochCount++

	e.epochStart = append(e.epochStart, e.clock.NowFunc()())
	e.idsByEpoch = append(e.idsByEpoch, nil)

	w := e.writer()

	for round := range roundsPerEpoch {
		for k := range 3 {
			data := fmt.Appendf(nil, "content-%d-%d-%d-%x", epochIndex, round, k, e.rnd.Int63())
			e.idsByEpoch[epochIndex] = append(e.idsByEpoch[epochIndex], e.writeContent(w, data))
		}

		if round == 0 && epochIndex >= 3 {
			for n, id := range e.idsByEpoch[epochIndex-3] {
				if n%3 != 2 {
					require.NoError(e.t, w.ContentManager().DeleteContent(e.ctx, id))
				}
			}
		}

		if round == 1 && epochIndex >= 6 && epochIndex%2 == 0 {
			for n, id := range e.idsByEpoch[epochIndex-6] {
				if n%3 == 0 {
					e.writeContent(w, e.dataByID[id])
				}
			}
		}

		e.simulateGuardScenario(epochIndex, round)

		require.NoError(e.t, w.Flush(e.ctx))
		e.clock.Advance(2 * time.Hour)
	}

	e.clock.Advance(25 * time.Hour)
	e.runEpochMaintenance()

	cs, err := e.epochManager().Current(e.ctx)
	require.NoError(e.t, err)

	e.t.Logf("PROGRESS simulated epoch %v: write epoch %v, %v range checkpoints, at %v",
		epochIndex, cs.WriteEpoch, len(cs.LongestRangeCheckpointSets), time.Now().Format("15:04:05.000")) //nolint:forbidigo
}

// simulateGuardScenario creates a deleted entry in the range checkpoints that wins over an older entry
// which another writer only flushes much later (into an epoch after the range checkpoints).
func (e *mergeTestEnv) simulateGuardScenario(epochIndex, round int) {
	if round != 2 {
		return
	}

	data := []byte("guard-content")

	switch epochIndex {
	case 10:
		w := e.env.MustOpenAnother(e.t, e.openOpt)
		dw, ok := w.(repo.DirectRepositoryWriter)
		require.True(e.t, ok)

		e.guardWriter = dw
		e.guardID = e.writeContent(dw, data)

		e.clock.Advance(time.Minute)
	case 11:
		require.Equal(e.t, e.guardID, e.writeContent(e.writer(), data))
	case 12:
		require.NoError(e.t, e.writer().ContentManager().DeleteContent(e.ctx, e.guardID))
	}
}

func (e *mergeTestEnv) flushGuardWriter() {
	require.NoError(e.t, e.guardWriter.Flush(e.ctx))
}

func (e *mergeTestEnv) advanceWatermark(ts time.Time) {
	_, err := e.epochManager().AdvanceDeletionWatermark(e.ctx, ts)
	require.NoError(e.t, err)
}

func (e *mergeTestEnv) merge(opt indexblob.MergeRangeOptions) (*indexblob.MergeRangeStats, error) {
	opt.Progress = e.t.Logf

	//nolint:wrapcheck
	return e.writer().ContentManager().IndexBlobManagerV1().MergeRangeCheckpoints(e.ctx, opt)
}

// visibleState is what a fresh reader sees: the listing including deleted entries and the lookup of every known content.
type visibleState struct {
	listed  map[content.ID]content.Info
	lookups map[content.ID]string
}

func (e *mergeTestEnv) readState(rep repo.Repository) visibleState {
	dr, ok := rep.(repo.DirectRepository)
	require.True(e.t, ok)

	state := visibleState{
		listed:  map[content.ID]content.Info{},
		lookups: map[content.ID]string{},
	}

	require.NoError(e.t, dr.ContentReader().IterateContents(e.ctx, content.IterateOptions{IncludeDeleted: true}, func(ci content.Info) error {
		state.listed[ci.ContentID] = ci
		return nil
	}))

	for id, data := range e.dataByID {
		ci, err := dr.ContentInfo(e.ctx, id)
		if err != nil {
			state.lookups[id] = "error: " + err.Error()
			continue
		}

		if ci.Deleted {
			state.lookups[id] = fmt.Sprintf("deleted %v %v", ci.TimestampSeconds, ci.PackBlobID)
			continue
		}

		got, err := dr.ContentReader().GetContent(e.ctx, id)
		require.NoError(e.t, err, "visible content must be readable: %v", id)
		require.Equal(e.t, data, got)

		state.lookups[id] = fmt.Sprintf("live %v %v", ci.TimestampSeconds, ci.PackBlobID)
	}

	return state
}

func (e *mergeTestEnv) readMainState() visibleState {
	rep, err := repo.Open(e.ctx, e.env.ConfigFile(), e.env.Password, &repo.Options{TimeNowFunc: e.clock.NowFunc()})
	require.NoError(e.t, err)

	defer rep.Close(e.ctx)

	return e.readState(rep)
}

// readStateWithoutBlobs reads a copy of the storage that does not contain the excluded blobs.
func (e *mergeTestEnv) readStateWithoutBlobs(excluded func(blob.ID) bool) visibleState {
	src := e.env.RootStorage()
	dst := repotesting.NewReconnectableStorage(e.t, blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, e.clock.NowFunc()))

	excludedCount := 0

	require.NoError(e.t, src.ListBlobs(e.ctx, "", func(bm blob.Metadata) error {
		if excluded(bm.BlobID) {
			excludedCount++
			return nil
		}

		var data gather.WriteBuffer
		defer data.Close()

		require.NoError(e.t, src.GetBlob(e.ctx, bm.BlobID, 0, -1, &data))
		require.NoError(e.t, dst.PutBlob(e.ctx, bm.BlobID, data.Bytes(), blob.PutOptions{}))

		return nil
	}))

	require.Positive(e.t, excludedCount, "the comparison is only meaningful if blobs were excluded")

	config := filepath.Join(testutil.TempDirectory(e.t), "kopia.config")
	require.NoError(e.t, repo.Connect(e.ctx, config, dst, e.env.Password, &repo.ConnectOptions{}))

	rep, err := repo.Open(e.ctx, config, e.env.Password, &repo.Options{TimeNowFunc: e.clock.NowFunc()})
	require.NoError(e.t, err)

	defer rep.Close(e.ctx)

	return e.readState(rep)
}

func (e *mergeTestEnv) blobCount() int {
	count := 0

	require.NoError(e.t, e.env.RootStorage().ListBlobs(e.ctx, "", func(blob.Metadata) error {
		count++
		return nil
	}))

	return count
}

func (e *mergeTestEnv) currentRanges() []*epoch.RangeMetadata {
	em := e.epochManager()
	require.NoError(e.t, em.Refresh(e.ctx))

	cs, err := em.Current(e.ctx)
	require.NoError(e.t, err)

	return cs.LongestRangeCheckpointSets
}

func hasPrefixOf(prefixes ...blob.ID) func(blob.ID) bool {
	return func(id blob.ID) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(string(id), string(p)) {
				return true
			}
		}

		return false
	}
}

func requireSameState(t *testing.T, want, got visibleState) {
	t.Helper()

	require.Len(t, got.listed, len(want.listed))
	require.Equal(t, want.listed, got.listed)
	require.Equal(t, want.lookups, got.lookups)
}

func TestMergeRangeCheckpoints(t *testing.T) {
	e := newMergeTestEnv(t)

	for range 40 {
		e.simulateEpoch()
	}

	rangesBefore := e.currentRanges()
	require.GreaterOrEqual(t, len(rangesBefore), 3, "test needs several range checkpoints")

	maxEpoch := rangesBefore[len(rangesBefore)-1].MaxEpoch
	t.Logf("range checkpoints before merge: %v, max epoch %v", len(rangesBefore), maxEpoch)

	// deletions before epoch 30 are below the watermark, later deletions are not
	e.advanceWatermark(e.epochStart[30])

	// the other writer flushes its old entry for the guard content into the current epoch
	e.flushGuardWriter()

	before := e.readMainState()
	require.Equal(t, "error: content not found", e.readMainState().lookups[e.guardID])

	// nothing must be written in dry-run mode, and the result must not depend on the partition count
	blobsBefore := e.blobCount()

	dry1, err := e.merge(indexblob.MergeRangeOptions{Partitions: 1, DryRun: true})
	require.NoError(t, err)

	dry7, err := e.merge(indexblob.MergeRangeOptions{Partitions: 7, DryRun: true})
	require.NoError(t, err)

	require.Equal(t, blobsBefore, e.blobCount(), "dry run must not write blobs")
	require.Equal(t, dry1, dry7)

	stats, err := e.merge(indexblob.MergeRangeOptions{Partitions: 4})
	require.NoError(t, err)

	t.Logf("merge stats: %+v", stats)

	// readers use the merged checkpoint now and must see exactly the same contents as before
	rangesAfter := e.currentRanges()
	require.Len(t, rangesAfter, 1)
	require.Equal(t, 0, rangesAfter[0].MinEpoch)
	require.Equal(t, maxEpoch, rangesAfter[0].MaxEpoch)

	after := e.readMainState()
	requireSameState(t, before, after)
	require.Equal(t, "error: content not found", after.lookups[e.guardID])

	require.Equal(t, 0, stats.MinEpoch)
	require.Equal(t, maxEpoch, stats.MaxEpoch)
	require.Equal(t, dry1.OutputEntries, stats.OutputEntries)
	require.Positive(t, stats.DroppedDeleted, "deleted entries below the watermark must be dropped")
	require.Positive(t, stats.KeptDeletedRecent, "deleted entries above the watermark must be kept")
	require.Equal(t, int64(1), stats.KeptDeletedGuard, "the guard content must keep its deleted entry")
	require.Equal(t, stats.UniqueContents, stats.OutputEntries+stats.DroppedDeleted)
	require.NotEmpty(t, stats.OutputBlobs)

	mergedPrefix := epoch.RangeCheckpointBlobPrefix(0, maxEpoch)

	// a second merge has nothing to do
	_, err = e.merge(indexblob.MergeRangeOptions{Partitions: 4})
	require.ErrorContains(t, err, "nothing to merge")

	// normal operation continues: new epochs, new range checkpoints, a later watermark
	for range 12 {
		e.simulateEpoch()
	}

	rangesLater := e.currentRanges()
	require.Greater(t, len(rangesLater), 1, "new range checkpoints must follow the merged one")
	require.Equal(t, maxEpoch, rangesLater[0].MaxEpoch)

	e.advanceWatermark(e.epochStart[45])

	// the merged checkpoint must give the same result as the original chain
	requireSameState(t, e.readStateWithoutBlobs(hasPrefixOf(mergedPrefix)), e.readMainState())

	// merging again also merges the first merged checkpoint
	stats2, err := e.merge(indexblob.MergeRangeOptions{Partitions: 3})
	require.NoError(t, err)

	t.Logf("second merge stats: %+v", stats2)

	mergedPrefix2 := epoch.RangeCheckpointBlobPrefix(0, stats2.MaxEpoch)

	require.Len(t, e.currentRanges(), 1)
	requireSameState(t, e.readStateWithoutBlobs(hasPrefixOf(mergedPrefix, mergedPrefix2)), e.readMainState())
	requireSameState(t, e.readStateWithoutBlobs(hasPrefixOf(mergedPrefix)), e.readMainState())

	// maintenance keeps working after the merge
	for range 9 {
		e.simulateEpoch()
	}

	require.Greater(t, len(e.currentRanges()), 1)
	requireSameState(t, e.readStateWithoutBlobs(hasPrefixOf(mergedPrefix, mergedPrefix2)), e.readMainState())
}

// TestMergeRangeCheckpointsDropsEverythingIgnored checks the size reduction: when everything is deleted below the watermark, nothing is left.
func TestMergeRangeCheckpointsDropsEverythingIgnored(t *testing.T) {
	ctx := testlogging.Context(t)
	e := newMergeTestEnv(t)

	for range 20 {
		e.simulateEpoch()
	}

	w := e.writer()
	for _, ids := range e.idsByEpoch {
		for _, id := range ids {
			if err := w.ContentManager().DeleteContent(ctx, id); err != nil {
				require.ErrorIs(t, err, content.ErrContentNotFound)
			}
		}
	}

	require.NoError(t, w.Flush(ctx))

	for range 12 {
		e.clock.Advance(25 * time.Hour)
		e.simulateEmptyEpoch()
	}

	e.advanceWatermark(e.clock.NowFunc()())
	e.flushGuardWriter()

	before := e.readMainState()

	stats, err := e.merge(indexblob.MergeRangeOptions{Partitions: 2})
	require.NoError(t, err)
	t.Logf("merge stats: %+v", stats)

	require.Equal(t, int64(1), stats.KeptDeletedGuard)
	require.Equal(t, int64(0), stats.KeptDeletedRecent)
	require.Equal(t, stats.KeptDeletedGuard, stats.OutputEntries, "only the guarded deleted entry may remain")

	requireSameState(t, before, e.readMainState())
}

// simulateEmptyEpoch writes only filler contents that are deleted right away, so that the epoch advances.
func (e *mergeTestEnv) simulateEmptyEpoch() {
	w := e.writer()

	for round := range roundsPerEpoch {
		id, err := w.ContentManager().WriteContent(e.ctx, gather.FromSlice(fmt.Appendf(nil, "filler-%d-%d", e.epochCount, round)), "", content.NoCompression)
		require.NoError(e.t, err)
		require.NoError(e.t, w.Flush(e.ctx))
		require.NoError(e.t, w.ContentManager().DeleteContent(e.ctx, id))
		require.NoError(e.t, w.Flush(e.ctx))
		e.clock.Advance(2 * time.Hour)
	}

	e.epochCount++
	e.epochStart = append(e.epochStart, e.clock.NowFunc()())
	e.idsByEpoch = append(e.idsByEpoch, nil)

	e.clock.Advance(25 * time.Hour)
	e.runEpochMaintenance()
}
