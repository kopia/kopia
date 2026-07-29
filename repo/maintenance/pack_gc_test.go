package maintenance_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
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
	"github.com/kopia/kopia/repo/maintenancestats"
	"github.com/kopia/kopia/repo/object"
)

var testHMACSecret = []byte{1, 2, 3}

var testMasterKey = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func (s *formatSpecificTestSuite) TestDeleteUnreferencedPacks(t *testing.T) {
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
	w := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{MetadataCompressor: "zstd-fastest"})
	io.WriteString(w, "hello world!")
	w.Result()
	w.Close()

	env.RepositoryWriter.Flush(ctx)

	blobsBefore, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")

	require.NoError(t, err)
	require.Len(t, blobsBefore, 4, "unexpected number of blobs after writing")

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
	_, err = maintenance.DeleteUnreferencedPacks(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedPacksOptions{}, maintenance.SafetyFull)
	require.NoError(t, err)

	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobID1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobID2)

	// mixed safety parameters
	safetyFastDeleteLongSessionExpiration := maintenance.SafetyParameters{
		PackDeleteMinAge:     1,
		SessionExpirationAge: 4 * 24 * time.Hour,
	}

	// new blobs will be deleted
	_, err = maintenance.DeleteUnreferencedPacks(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedPacksOptions{}, maintenance.SafetyNone)
	require.NoError(t, err)

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

	_, err = maintenance.DeleteUnreferencedPacks(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedPacksOptions{}, safetyFastDeleteLongSessionExpiration)
	require.NoError(t, err)

	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), session1Marker)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), session2Marker)

	// now finish session 2
	env.RepositoryWriter.BlobStorage().DeleteBlob(ctx, session2Marker)

	_, err = maintenance.DeleteUnreferencedPacks(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedPacksOptions{}, safetyFastDeleteLongSessionExpiration)
	require.NoError(t, err)

	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)
	verifyBlobExists(t, env.RepositoryWriter.BlobStorage(), session1Marker)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), session2Marker)

	// now move time into the future making session 1 timed out
	ta.Advance(7 * 24 * time.Hour)

	_, err = maintenance.DeleteUnreferencedPacks(ctx, env.RepositoryWriter, maintenance.DeleteUnreferencedPacksOptions{}, maintenance.SafetyFull)
	require.NoError(t, err)

	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession1)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession2)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), extraBlobIDWithSession3)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), session1Marker)
	verifyBlobNotFound(t, env.RepositoryWriter.BlobStorage(), session2Marker)

	// make sure we're back to the starting point.

	blobsAfter, err := blob.ListAllBlobs(ctx, env.RepositoryWriter.BlobStorage(), "")
	require.NoError(t, err)

	diff := cmp.Diff(blobsBefore, blobsAfter)
	require.Empty(t, diff, "unexpected blobs")
}

// TestDeleteUnreferencedPacksDoesNotDeadlockOnPersistentDeleteFailure covers a
// deadlock that is easy to hit and hard to diagnose. The delete workers leave
// their range loop as soon as one of them returns an error, while the producer
// keeps sending into a buffered channel. Once every worker is gone and the
// buffer is full, the send blocks forever: close(unused) and eg.Wait() are
// never reached, and the maintenance run sits there with no output and no CPU
// use until it is killed. SIGTERM does not clear it either, because the
// goroutine is parked on a channel send rather than on the context.
//
// The threshold is deleteQueueSize plus the worker count, so roughly 116 with
// the defaults, and it is unrelated to repository size. S3 Object Lock is
// simply the most reliable way to produce a persistent DeleteBlob failure;
// any storage that keeps rejecting deletes does the same, which is why this
// test runs without object lock enabled.
func TestDeleteUnreferencedPacksDoesNotDeadlockOnPersistentDeleteFailure(t *testing.T) {
	t.Parallel()

	var faulty *blobtesting.FaultyStorage

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		WrapStorage: func(st blob.Storage) blob.Storage {
			faulty = blobtesting.NewFaultyStorage(st)
			return faulty
		},
	})

	// Comfortably more than deleteQueueSize + parallelism, so the producer
	// still has blobs to hand over after the last worker has given up.
	const unreferencedBlobs = 250

	// Hex-only IDs on purpose: kopia parses pack blob IDs, and a name with
	// non-hex characters is skipped instead of being treated as an
	// unreferenced pack, which would make this test silently vacuous.
	for i := range unreferencedBlobs {
		mustPutDummyBlob(t, env.RepositoryWriter.BlobStorage(), blob.ID(fmt.Sprintf("pbeef%08x", i)))
	}

	// Every delete from here on is refused, standing in for a storage-level
	// retention policy or any other permanent denial.
	faulty.AddFault(blobtesting.MethodDeleteBlob).
		Repeat(10 * unreferencedBlobs).
		ErrorInstead(errors.New("AccessDenied"))

	type outcome struct {
		stats *maintenancestats.DeleteUnreferencedPacksStats
		err   error
	}

	done := make(chan outcome, 1)

	go func() {
		// SafetyNone so the blobs we just wrote are immediately eligible
		// instead of being retained by PackDeleteMinAge.
		s, err := maintenance.DeleteUnreferencedPacks(ctx, env.RepositoryWriter,
			maintenance.DeleteUnreferencedPacksOptions{}, maintenance.SafetyNone)
		done <- outcome{stats: s, err: err}
	}()

	select {
	case res := <-done:
		if res.err == nil && res.stats != nil {
			// Guard against a vacuous pass: if the GC pass found nothing to
			// delete, it never exercised the delete path at all and this
			// test proves nothing.
			require.Failf(t, "GC pass did not attempt any deletes",
				"unreferenced=%d retained=%d deleted=%d - the seeded blobs were not treated as unreferenced packs",
				res.stats.UnreferencedPackCount, res.stats.RetainedPackCount, res.stats.DeletedPackCount)
		}

		// Returning an error is the correct behaviour without object lock:
		// a delete that keeps failing is a real storage problem. What must
		// not happen is not returning at all.
		require.Error(t, res.err, "a persistent delete failure must fail the GC pass, not succeed")
	case <-time.After(30 * time.Second):
		t.Fatal("DeleteUnreferencedPacks did not return - the producer is deadlocked on the delete queue")
	}
}

func verifyBlobExists(t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	_, err := st.GetMetadata(testlogging.Context(t), blobID)
	require.NoError(t, err)
}

func verifyBlobNotFound(t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	_, err := st.GetMetadata(testlogging.Context(t), blobID)
	require.ErrorIsf(t, err, blob.ErrBlobNotFound, "expected blob %v to be not found", blobID)
}

func mustPutDummyBlob(t *testing.T, st blob.Storage, blobID blob.ID) {
	t.Helper()

	err := st.PutBlob(testlogging.Context(t), blobID, gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{})
	require.NoError(t, err)
}

func mustPutDummySessionBlob(t *testing.T, st blob.Storage, sessionIDSuffix blob.ID, si *content.SessionInfo) blob.ID {
	t.Helper()

	j, err := json.Marshal(si)
	require.NoError(t, err)

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

	require.NoError(t, e.Encrypt(gather.FromSlice(j), iv, &enc))
	require.NoError(t, st.PutBlob(testlogging.Context(t), blobID, enc.Bytes(), blob.PutOptions{}))

	return blobID
}
