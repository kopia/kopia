package maintenance

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

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/object"
)

var testHMACSecret = []byte{1, 2, 3}

func TestDeleteUnreferencedBlobs(t *testing.T) {
	ctx := testlogging.Context(t)

	var env repotesting.Environment

	ta := faketime.NewTimeAdvance(time.Now(), 1*time.Second)

	// setup repository without encryption and without HMAC so we can implant session blobs
	defer env.Setup(t, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = "NONE"
			nro.BlockFormat.Hash = "HMAC-SHA256"
			nro.BlockFormat.HMACSecret = testHMACSecret
		},
	}).Close(ctx, t)

	w := env.Repository.NewObjectWriter(ctx, object.WriterOptions{})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()

	env.Repository.Flush(ctx)

	blobsBefore, err := blob.ListAllBlobs(ctx, env.Repository.Blobs, "")
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(blobsBefore), 3; got != want {
		t.Fatalf("unexpected number of blobs after writing: %v", blobsBefore)
	}

	// add some more unreferenced blobs
	const (
		extraBlobID1 blob.ID = "pdeadbeef1"
		extraBlobID2 blob.ID = "pdeadbeef2"
	)

	mustPutDummyBlob(t, env.Repository.Blobs, extraBlobID1)
	mustPutDummyBlob(t, env.Repository.Blobs, extraBlobID2)
	verifyBlobExists(t, env.Repository.Blobs, extraBlobID1)
	verifyBlobExists(t, env.Repository.Blobs, extraBlobID2)

	// new blobs not will be deleted because of minimum age requirement
	if _, err = DeleteUnreferencedBlobs(ctx, env.Repository, DeleteUnreferencedBlobsOptions{
		MinAge: 1 * time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	verifyBlobExists(t, env.Repository.Blobs, extraBlobID1)
	verifyBlobExists(t, env.Repository.Blobs, extraBlobID2)

	// new blobs will be deleted
	if _, err = DeleteUnreferencedBlobs(ctx, env.Repository, DeleteUnreferencedBlobsOptions{
		MinAge: 1,
	}); err != nil {
		t.Fatal(err)
	}

	verifyBlobNotFound(t, env.Repository.Blobs, extraBlobID1)
	verifyBlobNotFound(t, env.Repository.Blobs, extraBlobID2)

	// add blobs again and
	const (
		extraBlobIDWithSession1 blob.ID = "pdeadbeef1-s01"
		extraBlobIDWithSession2 blob.ID = "pdeadbeef2-s01"
		extraBlobIDWithSession3 blob.ID = "pdeadbeef3-s02"
	)

	mustPutDummyBlob(t, env.Repository.Blobs, extraBlobIDWithSession1)
	mustPutDummyBlob(t, env.Repository.Blobs, extraBlobIDWithSession2)
	mustPutDummyBlob(t, env.Repository.Blobs, extraBlobIDWithSession3)

	session1Marker := mustPutDummySessionBlob(t, env.Repository.Blobs, "s01", &content.SessionInfo{
		CheckpointTime: ta.NowFunc()(),
	})
	session2Marker := mustPutDummySessionBlob(t, env.Repository.Blobs, "s02", &content.SessionInfo{
		CheckpointTime: ta.NowFunc()(),
	})

	if _, err = DeleteUnreferencedBlobs(ctx, env.Repository, DeleteUnreferencedBlobsOptions{
		MinAge: 1,
	}); err != nil {
		t.Fatal(err)
	}

	verifyBlobExists(t, env.Repository.Blobs, extraBlobIDWithSession1)
	verifyBlobExists(t, env.Repository.Blobs, extraBlobIDWithSession2)
	verifyBlobExists(t, env.Repository.Blobs, extraBlobIDWithSession3)
	verifyBlobExists(t, env.Repository.Blobs, session1Marker)
	verifyBlobExists(t, env.Repository.Blobs, session2Marker)

	// now finish session 2
	env.Repository.Blobs.DeleteBlob(ctx, session2Marker)

	if _, err = DeleteUnreferencedBlobs(ctx, env.Repository, DeleteUnreferencedBlobsOptions{
		MinAge: 1,
	}); err != nil {
		t.Fatal(err)
	}

	verifyBlobExists(t, env.Repository.Blobs, extraBlobIDWithSession1)
	verifyBlobExists(t, env.Repository.Blobs, extraBlobIDWithSession2)
	verifyBlobNotFound(t, env.Repository.Blobs, extraBlobIDWithSession3)
	verifyBlobExists(t, env.Repository.Blobs, session1Marker)
	verifyBlobNotFound(t, env.Repository.Blobs, session2Marker)

	// now move time into the future making session 1 timed out
	ta.Advance(7 * 24 * time.Hour)

	if _, err = DeleteUnreferencedBlobs(ctx, env.Repository, DeleteUnreferencedBlobsOptions{
		MinAge: 1,
	}); err != nil {
		t.Fatal(err)
	}

	verifyBlobNotFound(t, env.Repository.Blobs, extraBlobIDWithSession1)
	verifyBlobNotFound(t, env.Repository.Blobs, extraBlobIDWithSession2)
	verifyBlobNotFound(t, env.Repository.Blobs, extraBlobIDWithSession3)
	verifyBlobNotFound(t, env.Repository.Blobs, session1Marker)
	verifyBlobNotFound(t, env.Repository.Blobs, session2Marker)

	// make sure we're back to the starting point.

	blobsAfter, err := blob.ListAllBlobs(ctx, env.Repository.Blobs, "")
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(blobsBefore, blobsAfter); diff != "" {
		t.Errorf("unexpected diff: %v", diff)
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

	if err := st.PutBlob(testlogging.Context(t), blobID, gather.FromSlice([]byte{1, 2, 3})); err != nil {
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

	blobID := blob.ID(fmt.Sprintf("s%x-%v", h.Sum(nil)[16:32], sessionIDSuffix))

	if err := st.PutBlob(testlogging.Context(t), blobID, gather.FromSlice(j)); err != nil {
		t.Fatal(err)
	}

	return blobID
}
