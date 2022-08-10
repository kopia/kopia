package maintenance_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
)

var testHMACSecret = []byte{1, 2, 3}

var testMasterKey = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func (s *formatSpecificTestSuite) TestDeleteUnreferencedBlobs(t *testing.T) {
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

	// add some more unreferenced blobs
	const (
		extraBlobID1 blob.ID = "pdeadbeef1"
		extraBlobID2 blob.ID = "pdeadbeef2"
	)

	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), extraBlobID1)
	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), extraBlobID2)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobID1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobID2)

	// new blobs not will be deleted because of minimum age requirement
	if _, err = maintenance.DeleteUnreferencedBlobs(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedBlobsOptions{}, maintenance.SafetyFull); err != nil {
		t.Fatal(err)
	}

	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobID1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobID2)

	// mixed safety parameters
	safetyFastDeleteLongSessionExpiration := maintenance.SafetyParameters{
		BlobDeleteMinAge:     1,
		SessionExpirationAge: 4 * 24 * time.Hour,
	}

	// new blobs will be deleted
	if _, err = maintenance.DeleteUnreferencedBlobs(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedBlobsOptions{}, maintenance.SafetyNone); err != nil {
		t.Fatal(err)
	}

	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobID1)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobID2)

	// add blobs again and
	const (
		extraBlobIDWithSession1 blob.ID = "pdeadbeef1-s01"
		extraBlobIDWithSession2 blob.ID = "pdeadbeef2-s01"
		extraBlobIDWithSession3 blob.ID = "pdeadbeef3-s02"
	)

	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)

	session1Marker := mustPutDummySessionBlob(t, env.RepositoryWriter.BlobStorage(), "s01", &content.SessionInfo{
		CheckpointTime: clock.Now(),
	})
	session2Marker := mustPutDummySessionBlob(t, env.RepositoryWriter.BlobStorage(), "s02", &content.SessionInfo{
		CheckpointTime: ta.NowFunc()(),
	})

	if _, err = maintenance.DeleteUnreferencedBlobs(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedBlobsOptions{}, safetyFastDeleteLongSessionExpiration); err != nil {
		t.Fatal(err)
	}

	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), session1Marker)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), session2Marker)

	// now finish session 2
	env.RepositoryWriter.BlobStorage().DeleteBlob(ctx, session2Marker)

	if _, err = maintenance.DeleteUnreferencedBlobs(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedBlobsOptions{}, safetyFastDeleteLongSessionExpiration); err != nil {
		t.Fatal(err)
	}

	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), session1Marker)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), session2Marker)

	// now move time into the future making session 1 timed out
	ta.Advance(7 * 24 * time.Hour)

	if _, err = maintenance.DeleteUnreferencedBlobs(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedBlobsOptions{}, maintenance.SafetyFull); err != nil {
		t.Fatal(err)
	}

	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), session1Marker)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), session2Marker)

	// make sure we're back to the starting point.

	blobsAfter, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(blobsBefore, blobsAfter); diff != "" {
		t.Fatalf("unexpected diff: %v", diff)
	}
}

func verifyBlobExists(t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	if _, err := st.GetMetadata(testlogging.Context(t), blobID); err != nil {
		t.Fatalf("expected blob %v to exist, got %v", blobID, err)
	}
}

func verifyBlobNotFound(t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	if _, err := st.GetMetadata(testlogging.Context(t), blobID); !errors.Is(err, blob.ErrBlobNotFound) {
		t.Fatalf("expected blob %v to be not found, got %v", blobID, err)
	}
}

func mustPutDummyBlob(t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	if err := st.PutBlob(testlogging.Context(t), blobID, gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

func mustPutDummySessionBlob(t *testing.T, st blob.Storage, sessionIDSuffix blob.ID, si *content.SessionInfo) blob.ID {
	t.Helper()

	j, err := json.Marshal(si)
	if err != nil {
		t.Fatal(err)
	}

	h := hmac.New(sha256.New, testHMACSecret)
	h.Write(j)

	iv := h.Sum(nil)[16:32]

	blobID := blob.ID(fmt.Sprintf("s%x-%v", iv, sessionIDSuffix))

	e, err := encryption.CreateEncryptor(&format.ContentFormat{
		Encryption: encryption.DefaultAlgorithm,
		MasterKey:  testMasterKey,
		HMACSecret: testHMACSecret,
	})

	require.NoError(t, err)

	var enc gather.WriteBuffer
	defer enc.Close()

	require.NoError(t, e.ToRepository(gather.FromSlice(j), iv, &enc))
	require.NoError(t, st.PutBlob(testlogging.Context(t), blobID, enc.Bytes(), blob.PutOptions{}))

	return blobID
}
