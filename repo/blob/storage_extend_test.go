package blob_test

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/object"
)

var testHMACSecret = []byte{1, 2, 3}

var testMasterKey = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func (s *formatSpecificTestSuite) TestExtendBlobRetention(t *testing.T) {
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
			nro.BlockFormat.Hash = "HMAC-SHA256"
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

	if got, want := len(blobsBefore), 4; got != want {
		t.Fatalf("unexpected number of blobs after writing: %v", blobsBefore)
	}

	lastBlobIdx := len(blobsBefore) - 1
	st := env.RootStorage().(blobtesting.RetentionStorage)

	// Verify that file is locked
	gotMode, expiry, err := st.GetRetention(ctx, blobsBefore[lastBlobIdx].BlobID)
	require.NoError(t, err, "getting blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)

	period = 2 * time.Hour
	earliestExpiry = ta.NowFunc()().Add(period)

	// Relock blob
	err = env.RepositoryWriter.BlobStorage().ExtendBlobRetention(ctx, blobsBefore[lastBlobIdx].BlobID, blob.ExtendOptions{
		RetentionMode:   mode,
		RetentionPeriod: period,
	})
	require.NoErrorf(t, err, "Extending Retention time failed, got err: %v", err)

	// Verify Lock period
	gotMode, expiry, err = st.GetRetention(ctx, blobsBefore[lastBlobIdx].BlobID)
	require.NoError(t, err, "getting blob retention info")

	assert.Equal(t, mode, gotMode)
	assert.WithinDuration(t, earliestExpiry, expiry, time.Minute)
}

func (s *formatSpecificTestSuite) TestExtendBlobRetentionUnsupported(t *testing.T) {
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
			nro.BlockFormat.Hash = "HMAC-SHA256"
			nro.BlockFormat.HMACSecret = testHMACSecret
			// Ensure retention is disabled to trigger ExtendBlobRetention unsupported
			nro.RetentionPeriod = 0
			nro.RetentionMode = ""
		},
	})
	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()

	env.RepositoryWriter.Flush(ctx)

	blobsBefore, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(blobsBefore), 4; got != want {
		t.Fatalf("unexpected number of blobs after writing: %v", blobsBefore)
	}

	lastBlobIdx := len(blobsBefore) - 1

	// Extend retention time
	err = env.RepositoryWriter.BlobStorage().ExtendBlobRetention(ctx, blobsBefore[lastBlobIdx].BlobID, blob.ExtendOptions{
		RetentionMode:   blob.Governance,
		RetentionPeriod: 2 * time.Hour,
	})
	require.EqualErrorf(t, err, "object locking unsupported", "Storage should not support ExtendBlobRetention")
}
