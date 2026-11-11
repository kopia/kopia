package indexblob

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
)

type compactionEpochParameters struct {
	*epoch.Parameters
}

func (p compactionEpochParameters) GetParameters(context.Context) (*epoch.Parameters, error) {
	return p.Parameters, nil
}

// Only the compactor's encryption reader uses this wrapper. The second input
// read pauses after the first input has populated and spilled the builder.
type pausedCompactionStorage struct {
	blob.Storage
	reads   int
	started chan struct{}
	release chan struct{}
}

func (s *pausedCompactionStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	s.reads++
	if s.reads == 2 {
		close(s.started)

		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.Storage.GetBlob(ctx, id, offset, length, output)
}

func writeConcurrentEpochIndex(t *testing.T, ctx context.Context, m *ManagerV1, entries []index.Info, session int) []blob.ID {
	t.Helper()

	b := index.NewOneUseBuilder()
	for _, ent := range entries {
		b.Add(ent)
	}

	shards, cleanup, err := b.BuildShards(index.Version2, true, DefaultIndexShardSize)
	require.NoError(t, err)

	defer cleanup()

	written, err := m.WriteIndexBlobs(ctx, shards, blob.ID(fmt.Sprintf("s%d", session)))
	require.NoError(t, err)

	return blob.IDsFromMetadata(written)
}

func TestCompactEpochBoundedConcurrentWritesAndEpochAdvance(t *testing.T) {
	ctx, cancel := context.WithTimeout(testlogging.Context(t), 30*time.Second)
	defer cancel()

	ft := faketime.NewTimeAdvance(fakeLocalStartTime)
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, ft.NowFunc())
	v0 := newIndexBlobManagerForTesting(t, st, ft.NowFunc())
	p := epoch.DefaultParameters()
	p.EpochAdvanceOnCountThreshold = 10
	p.MinEpochDuration = 12 * time.Hour
	p.CleanupSafetyMargin = 48 * time.Hour
	p.FullCheckpointFrequency = 1
	provider := compactionEpochParameters{&p}
	formatting := &format.ContentFormat{IndexVersion: index.Version2}

	compactor := NewManagerV1(st, v0.enc, nil, ft.NowFunc(), formatting, nil, "", 0)
	compactor.epochMgr = epoch.NewManager(st, provider, compactor.CompactEpoch, nil, ft.NowFunc())
	writer := NewManagerV1(st, v0.enc, nil, ft.NowFunc(), formatting, nil, "", 0)
	writer.epochMgr = epoch.NewManager(st, provider, writer.CompactEpoch, nil, ft.NowFunc())
	t.Cleanup(compactor.epochMgr.Flush)
	t.Cleanup(writer.epochMgr.Flush)

	entries := epochCompactionTestEntries(t, 800)

	var allInputs, oldInputs []blob.ID

	session := 0
	writeAndAdvance := func(epochNumber int) {
		for i := range 10 {
			if i == 9 {
				ft.Advance(p.MinEpochDuration + time.Hour)
			}

			part := entries[768+epochNumber*10+i : 769+epochNumber*10+i]
			if epochNumber == 0 {
				part = entries[:768]
				for k := range part {
					part[k].TimestampSeconds++
				}
			}

			session++
			ids := writeConcurrentEpochIndex(t, ctx, writer, part, session)

			allInputs = append(allInputs, ids...)
			if epochNumber == 0 {
				oldInputs = append(oldInputs, ids...)
			}
		}

		require.NoError(t, writer.epochMgr.Refresh(ctx))
		stats, err := writer.epochMgr.MaybeAdvanceWriteEpoch(ctx)
		require.NoError(t, err)
		require.True(t, stats.WasAdvanced)
		require.EqualValues(t, epochNumber+1, stats.CurrentEpoch)
		require.NoError(t, writer.epochMgr.Refresh(ctx))
	}

	writeAndAdvance(0)
	writeAndAdvance(1)
	// Epoch 0 is now settled. Pause real disk-backed compaction while another
	// client reads, writes and advances the current write epoch from 2 to 3.
	paused := &pausedCompactionStorage{Storage: st, started: make(chan struct{}), release: make(chan struct{})}
	enc := *v0.enc
	enc.st = paused
	compactor.enc = &enc

	var releaseOnce sync.Once

	release := func() { releaseOnce.Do(func() { close(paused.release) }) }
	defer release()

	tempDir := t.TempDir()
	compactor.cacheDirectory = tempDir
	writer.cacheDirectory = tempDir
	compactionCtx := WithEpochCompactionOptions(ctx, EpochCompactionOptions{MemoryLimitBytes: testEpochCompactionMemory})

	done := make(chan error, 1)
	go func() {
		_, err := compactor.epochMgr.MaybeCompactSingleEpoch(compactionCtx)
		done <- err
	}()

	select {
	case <-paused.started:
	case err := <-done:
		t.Fatalf("compaction finished before reading the second input: %v", err)
	case <-ctx.Done():
		t.Fatal("compaction did not reach the input read")
	}

	writeAndAdvance(2)

	session++
	allInputs = append(allInputs, writeConcurrentEpochIndex(t, ctx, writer, entries[799:800], session)...)

	// A reader sees all original and newly written data while the compacted set
	// does not exist yet. Cleanup must retain inputs of the in-flight compaction.
	verify := func() {
		require.NoError(t, writer.epochMgr.Refresh(ctx))
		active, _, err := writer.ListActiveIndexBlobs(ctx)
		require.NoError(t, err)

		var activeIDs []blob.ID
		for _, bm := range active {
			activeIDs = append(activeIDs, bm.BlobID)
		}

		require.Equal(t,
			epochCompactionExpected(t, ctx, writer, allInputs, index.Version2),
			epochCompactionExpected(t, ctx, writer, activeIDs, index.Version2))
	}
	verify()

	_, err := writer.epochMgr.CleanupSupersededIndexes(ctx)
	require.NoError(t, err)
	requireEpochCompactionOriginals(t, ctx, writer, oldInputs)

	release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("compaction did not finish after releasing the input read")
	}

	verify()

	outputs, err := blob.ListAllBlobs(ctx, st, epoch.SingleEpochCompactionBlobPrefix)
	require.NoError(t, err)
	require.Greater(t, len(outputs), 1)
	requireEpochCompactionOriginals(t, ctx, writer, oldInputs)
	requireEpochCompactionTempEmpty(t, tempDir)

	// The same bounded implementation also merges a compacted epoch with a
	// still-uncompacted epoch when producing a range checkpoint.
	rangeStats, err := writer.epochMgr.MaybeGenerateRangeCheckpoint(compactionCtx)
	require.NoError(t, err)
	require.NotNil(t, rangeStats)
	require.EqualValues(t, 0, rangeStats.RangeMinEpoch)
	require.EqualValues(t, 1, rangeStats.RangeMaxEpoch)
	verify()

	rangeOutputs, err := blob.ListAllBlobs(ctx, st, epoch.RangeCheckpointIndexBlobPrefix)
	require.NoError(t, err)
	require.Greater(t, len(rangeOutputs), 1)
	requireEpochCompactionTempEmpty(t, tempDir)
}

var _ blob.Storage = (*pausedCompactionStorage)(nil)
