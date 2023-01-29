package maintenance_test

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
			nro.RetentionMode = blob.Governance
			nro.RetentionPeriod = time.Hour * 24
		},
	})
	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
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

	st := env.RootStorage().(cache.Storage)

	ta.Advance(7 * 24 * time.Hour)

	if err = st.TouchBlob(ctx, blobsBefore[3].BlobID, time.Hour); err != nil {
		t.Fatalf("Altering expired object failed")
	}

	// extend retention time of all blobs
	if _, err = maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter, maintenance.ExtendBlobRetentionTimeOptions{}, maintenance.SafetyNone); err != nil {
		t.Fatal(err)
	}

	err = st.TouchBlob(ctx, blobsBefore[3].BlobID, time.Hour)
	assert.EqualErrorf(t, err, "cannot alter object before retention period expires", "Altering locked object should fail")
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
	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
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

	st := env.RootStorage().(cache.Storage)

	ta.Advance(7 * 24 * time.Hour)

	if err = st.TouchBlob(ctx, blobsBefore[3].BlobID, time.Hour); err != nil {
		t.Fatalf("Altering expired object failed")
	}

	// extend retention time of all blobs
	if _, err = maintenance.ExtendBlobRetentionTime(ctx, env.RepositoryWriter, maintenance.ExtendBlobRetentionTimeOptions{}, maintenance.SafetyNone); err != nil {
		t.Fatal(err)
	}

	if err = st.TouchBlob(ctx, blobsBefore[3].BlobID, time.Hour); err != nil {
		t.Fatalf("Altering expired object failed")
	}
}
