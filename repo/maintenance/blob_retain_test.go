package maintenance_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
)

const blockFormatHash = "HMAC-SHA256"

func (s *formatSpecificTestSuite) TestExtendBlobRetentionTime(t *testing.T) {
	mode := blob.Governance
	period := time.Hour * 24

	// set up fake clock which is initially synchronized to wall clock time
	// and moved at the same speed but which can be moved forward.
	ta := faketime.NewClockTimeWithOffset(0)
	earliestExpiry := ta.NowFunc()().Add(period)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = encryption.DefaultAlgorithm
			nro.BlockFormat.MasterKey = testMasterKey
			nro.BlockFormat.Hash = blockFormatHash
			nro.BlockFormat.HMACSecret = testHMACSecret
			nro.RetentionMode = mode
			nro.RetentionPeriod = period
		},
	})
	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()

	env.RepositoryWriter.Flush(ctx)

	blobsBefore, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")

	require.NoError(t, err)
	require.Len(t, blobsBefore, 4, "unexpected number of blobs after writing")

	lastBlobIdx := len(blobsBefore) - 1
	st := testutil.EnsureType[blobtesting.RetentionStorage](t, env.RootStorage())

	gotMode, expiry, err := st.GetRetention(ctx, blobsBefore[lastBlobIdx].BlobID)
	require.NoError(t, err, "getting blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)

	// Advance the clock and get a new earliestExpiry so we can attempt extending
	// retention and then check our blob again.
	ta.Advance(7 * 24 * time.Hour)
	earliestExpiry = ta.NowFunc()().Add(period)

	// extend retention time of all blobs
	stats, err := maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter, maintenance.ExtendBlobRetentionTimeOptions{}, maintenance.SafetyFull)
	require.NoError(t, err)
	require.EqualValues(t, 4, stats.ToExtendBlobCount)
	require.EqualValues(t, 4, stats.ExtendedBlobCount)
	require.Equal(t, "24h0m0s", stats.RetentionPeriod)

	gotMode, expiry, err = st.GetRetention(ctx, blobsBefore[lastBlobIdx].BlobID)
	require.NoError(t, err, "getting blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)
}

// recordingStorage notes which blobs had their retention extended, so a test
// can assert on what was left alone rather than only on counts.
type recordingStorage struct {
	blob.Storage

	mu       sync.Mutex
	extended map[blob.ID]int
}

func (s *recordingStorage) ExtendBlobRetention(ctx context.Context, blobID blob.ID, opts blob.ExtendOptions) error {
	s.mu.Lock()

	if s.extended == nil {
		s.extended = map[blob.ID]int{}
	}

	s.extended[blobID]++
	s.mu.Unlock()

	return s.Storage.ExtendBlobRetention(ctx, blobID, opts)
}

func (s *recordingStorage) wasExtended(blobID blob.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.extended[blobID] > 0
}

// TestExtendBlobRetentionTimeSkipsReclaimablePacks pins down the behaviour that
// makes rolling object-lock retention safe to switch on.
//
// Extending retention on every locking-prefix blob unconditionally looks
// harmless but is not: the extend step runs after orphaned-pack GC in the full
// maintenance cycle, so a pack that GC could not delete - under a
// compliance-mode lock it never can, that is the point - gets its expiry
// pushed out again on the next cycle, and the one after that. Such a blob can
// then never be reclaimed by anyone, and the repository grows monotonically
// with no way back.
//
// So reclaimable packs must be skipped, while index, epoch and format blobs
// must still be extended: those are not reclaimable and have to stay locked
// for as long as the repository exists.
func TestExtendBlobRetentionTimeSkipsReclaimablePacks(t *testing.T) {
	t.Parallel()

	const period = 24 * time.Hour

	var rec *recordingStorage

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = encryption.DefaultAlgorithm
			nro.BlockFormat.MasterKey = testMasterKey
			nro.BlockFormat.Hash = blockFormatHash
			nro.BlockFormat.HMACSecret = testHMACSecret
			// Retention has to be enabled or extendBlobRetentionTime returns
			// early and the test would pass without proving anything.
			nro.RetentionMode = blob.Governance
			nro.RetentionPeriod = period
		},
		WrapStorage: func(st blob.Storage) blob.Storage {
			rec = &recordingStorage{Storage: st}
			return rec
		},
	})

	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	// Blobs written by the flush above are referenced, so none of them is
	// reclaimable and all of them must keep being extended.
	referenced, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")
	require.NoError(t, err)
	require.NotEmpty(t, referenced)

	// An unreferenced pack blob: exactly what GC reclaims, and what extending
	// would immortalise.
	// Hex-only: kopia skips pack blob IDs it cannot parse, which would
	// leave the orphan out of the unreferenced set and make this test vacuous.
	const orphan blob.ID = "pdead0000000000a1"

	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), orphan)

	// SafetyNone so the orphan counts as reclaimable straight away instead of
	// being held back by PackDeleteMinAge.
	stats, err := maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter,
		maintenance.ExtendBlobRetentionTimeOptions{}, maintenance.SafetyNone)
	require.NoError(t, err)
	require.NotNil(t, stats)

	require.False(t, rec.wasExtended(orphan),
		"a reclaimable pack blob must not have its retention extended, or GC can never delete it")
	require.EqualValues(t, 1, stats.SkippedReclaimableBlobCount,
		"the skipped blob must be reported, so an operator can see the repository can still shrink")

	for _, bm := range referenced {
		require.True(t, rec.wasExtended(bm.BlobID),
			"referenced/metadata blob %v must still be extended", bm.BlobID)
	}
}

// TestExtendBlobRetentionTimeExtendsYoungOrphans is the counterpart: an
// unreferenced pack that GC would not touch yet must still be extended.
// Skipping it would be the opposite failure - letting protection lapse on data
// that is not yet provably garbage.
func TestExtendBlobRetentionTimeExtendsYoungOrphans(t *testing.T) {
	t.Parallel()

	const period = 24 * time.Hour

	var rec *recordingStorage

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = encryption.DefaultAlgorithm
			nro.BlockFormat.MasterKey = testMasterKey
			nro.BlockFormat.Hash = blockFormatHash
			nro.BlockFormat.HMACSecret = testHMACSecret
			nro.RetentionMode = blob.Governance
			nro.RetentionPeriod = period
		},
		WrapStorage: func(st blob.Storage) blob.Storage {
			rec = &recordingStorage{Storage: st}
			return rec
		},
	})

	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	const orphan blob.ID = "pdead0000000000a2"

	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), orphan)

	// SafetyFull keeps the freshly written orphan below PackDeleteMinAge, so
	// GC would not delete it in this cycle.
	stats, err := maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter,
		maintenance.ExtendBlobRetentionTimeOptions{}, maintenance.SafetyFull)
	require.NoError(t, err)
	require.NotNil(t, stats)

	require.True(t, rec.wasExtended(orphan),
		"an orphan that GC would not delete yet must keep its retention extended")
	require.Zero(t, stats.SkippedReclaimableBlobCount)
}

func (s *formatSpecificTestSuite) TestExtendBlobRetentionTimeDisabled(t *testing.T) {
	// set up fake clock which is initially synchronized to wall clock time
	// and moved at the same speed but which can be moved forward.
	ta := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = encryption.DefaultAlgorithm
			nro.BlockFormat.MasterKey = testMasterKey
			nro.BlockFormat.Hash = blockFormatHash
			nro.BlockFormat.HMACSecret = testHMACSecret
		},
	})
	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()

	env.RepositoryWriter.Flush(ctx)

	blobsBefore, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")

	require.NoError(t, err)
	require.Len(t, blobsBefore, 4, "unexpected number of blobs after writing")

	// Need to continue using TouchBlob because the environment only supports the
	// locking map if no retention time is given.
	lastBlobIdx := len(blobsBefore) - 1
	st := testutil.EnsureType[cache.Storage](t, env.RootStorage())

	ta.Advance(7 * 24 * time.Hour)

	_, err = st.TouchBlob(ctx, blobsBefore[lastBlobIdx].BlobID, time.Hour)
	require.NoError(t, err, "Altering expired object failed")

	// extend retention time of all blobs
	stats, err := maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter, maintenance.ExtendBlobRetentionTimeOptions{}, maintenance.SafetyFull)
	require.NoError(t, err)
	require.Nil(t, stats)

	_, err = st.TouchBlob(ctx, blobsBefore[lastBlobIdx].BlobID, time.Hour)
	require.NoError(t, err, "Altering expired object failed")
}
