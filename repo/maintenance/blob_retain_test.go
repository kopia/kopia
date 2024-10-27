package maintenance_test

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
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
	st := env.RootStorage().(blobtesting.RetentionStorage)

	gotMode, expiry, err := st.GetRetention(ctx, blobsBefore[lastBlobIdx].BlobID)
	require.NoError(t, err, "getting blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)

	// Advance the clock and get a new earliestExpiry so we can attempt extending
	// retention and then check our blob again.
	ta.Advance(7 * 24 * time.Hour)
	earliestExpiry = ta.NowFunc()().Add(period)

	// extend retention time of all blobs
	_, err = maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter, maintenance.ExtendBlobRetentionTimeOptions{})
	require.NoError(t, err)

	gotMode, expiry, err = st.GetRetention(ctx, blobsBefore[lastBlobIdx].BlobID)
	require.NoError(t, err, "getting blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)
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
	st := env.RootStorage().(cache.Storage)

	ta.Advance(7 * 24 * time.Hour)

	_, err = st.TouchBlob(ctx, blobsBefore[lastBlobIdx].BlobID, time.Hour)
	require.NoError(t, err, "Altering expired object failed")

	// extend retention time of all blobs
	_, err = maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter, maintenance.ExtendBlobRetentionTimeOptions{})
	require.NoError(t, err)

	_, err = st.TouchBlob(ctx, blobsBefore[lastBlobIdx].BlobID, time.Hour)
	require.NoError(t, err, "Altering expired object failed")
}
