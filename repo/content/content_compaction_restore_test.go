package content

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
)

func TestMinimumCompactionBudgetPreservesReaderShards(t *testing.T) {
	ctx := testlogging.Context(t)
	builder, err := index.NewSpillingBuilder(indexblob.MinimumEpochCompactionMemoryLimit, t.TempDir())
	require.NoError(t, err)

	defer builder.Close()

	// Maximum-length pack IDs give the lowest number of entries per shard.
	for n := range 1000 {
		id, err := index.ParseID(fmt.Sprintf("%032x", n))
		require.NoError(t, err)
		require.NoError(t, builder.Add(ctx, index.Info{
			ContentID:        id,
			PackBlobID:       blob.ID("p" + strings.Repeat("a", 254)),
			TimestampSeconds: 1700000000,
			OriginalLength:   1,
			PackedLength:     29,
			FormatVersion:    1,
		}))
	}

	var shards index.Merged
	defer func() { require.NoError(t, shards.Close()) }()

	require.NoError(t, builder.BuildShards(ctx, index.Version2, indexblob.DefaultIndexShardSize, func(data gather.Bytes) error {
		ndx, err := index.Open(append([]byte(nil), data.ToByteSlice()...), nil, func() int { return 28 })
		if err != nil {
			return err
		}

		shards = append(shards, ndx)

		return nil
	}))
	require.Greater(t, len(shards), 1)

	for _, ndx := range shards[:len(shards)-1] {
		require.GreaterOrEqual(t, ndx.ApproximateCount(), smallIndexEntryCountThreshold)
	}

	suite := &contentManagerSuite{mutableParameters: format.MutableParameters{
		Version: 2, IndexVersion: index.Version2, MaxPackSize: 1 << 20,
	}}
	manager := suite.newTestContentManager(t, blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil))
	combined, err := manager.committedContents.combineSmallIndexes(ctx, shards)
	require.NoError(t, err)
	require.Len(t, combined, len(shards), "readers must not recombine the compacted contents into one in-memory index")
}

func TestRestoreContentAfterSpilledEpochCompaction(t *testing.T) {
	ctx := testlogging.Context(t)
	ft := faketime.NewTimeAdvance(fakeTime)
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, ft.NowFunc())
	p := epoch.DefaultParameters()
	p.EpochAdvanceOnCountThreshold = 10
	p.MinEpochDuration = 12 * time.Hour
	suite := &contentManagerSuite{mutableParameters: format.MutableParameters{
		Version:         2,
		IndexVersion:    index.Version2,
		MaxPackSize:     1 << 20,
		EpochParameters: p,
	}}
	tempDir := t.TempDir()
	tweaks := &contentManagerTestTweaks{
		IndexCompactionMemoryLimitBytes: indexblob.MinimumEpochCompactionMemoryLimit,
		CacheDirectory:                  tempDir,
		ContentCacheSizeBytes:           16 << 20,
		MetadataCacheSizeBytes:          16 << 20,
		TimeNow:                         ft.NowFunc(),
		formatVersion:                   2,
	}
	writer := suite.newTestContentManagerWithTweaks(t, st, tweaks)
	em, enabled, err := writer.EpochManager(ctx)
	require.NoError(t, err)
	require.True(t, enabled)

	want := map[ID][]byte{}

	seed := 0
	for epochNumber := range 2 {
		for batch := range 10 {
			if batch == 9 {
				ft.Advance(p.MinEpochDuration + time.Hour)
			}

			count := 1
			if epochNumber == 0 {
				count = 64
			}

			for range count {
				data := seededRandomData(seed, 16+seed%512)
				id, err := writer.WriteContent(ctx, gather.FromSlice(data), "", NoCompression)
				require.NoError(t, err)

				want[id] = data
				seed++
			}

			require.NoError(t, writer.Flush(ctx))
		}

		require.NoError(t, em.Refresh(ctx))
		stats, err := em.MaybeAdvanceWriteEpoch(ctx)
		require.NoError(t, err)
		require.True(t, stats.WasAdvanced)
		require.EqualValues(t, epochNumber+1, stats.CurrentEpoch)
		require.NoError(t, em.Refresh(ctx))
	}

	verify := func(reader *WriteManager) {
		for id, data := range want {
			restored, err := reader.GetContent(ctx, id)
			require.NoError(t, err, "restoring content %v", id)
			require.Equal(t, data, restored, "restoring content %v", id)
		}
	}
	before := suite.newTestContentManagerWithTweaks(t, st, tweaks)
	verify(before)

	originals, err := blob.ListAllBlobs(ctx, st, epoch.UncompactedEpochBlobPrefix(0))
	require.NoError(t, err)
	require.Len(t, originals, 10)
	// Automatic maintenance has no per-run override: the local budget must
	// reach the epoch manager through the content manager's caching options.
	stats, err := em.MaybeCompactSingleEpoch(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	require.Zero(t, stats.Epoch)

	outputs, err := blob.ListAllBlobs(ctx, st, epoch.SingleEpochCompactionBlobPrefix)
	require.NoError(t, err)
	require.Greater(t, len(outputs), 1, "the small memory budget must force several output shards")

	// Remove the superseded indexes only in this in-memory test repository, as
	// eventual cleanup would. A fresh reader must restore actual encrypted pack
	// bytes using the newly compacted indexes, without old index/cache fallback.
	require.NoError(t, blob.DeleteMultiple(ctx, st, blob.IDsFromMetadata(originals), 1))
	// Disable local caches for the fresh reader to prevent cache fallback.
	freshTweaks := *tweaks
	freshTweaks.CachingOptions = CachingOptions{}
	after := suite.newTestContentManagerWithTweaks(t, st, &freshTweaks)
	require.NoError(t, after.Refresh(ctx))
	verify(after)

	remaining, err := blob.ListAllBlobs(ctx, st, epoch.UncompactedEpochBlobPrefix(0))
	require.NoError(t, err)
	require.Empty(t, remaining)

	temporary, err := os.ReadDir(filepath.Join(tempDir, "index-compaction"))
	require.NoError(t, err)
	require.Empty(t, temporary)
}
