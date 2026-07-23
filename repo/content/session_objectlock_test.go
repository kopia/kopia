package content

import (
	"bytes"
	"testing"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
)

func objectLockTestFormatProvider(t *testing.T) format.Provider {
	t.Helper()

	return mustCreateFormatProvider(t, &format.ContentFormat{
		Hash:       "HMAC-SHA256-128",
		Encryption: "AES256-GCM-HMAC-SHA256",
		HMACSecret: []byte("foo"),
		MasterKey:  []byte("0123456789abcdef0123456789abcdef"),
		MutableParameters: format.MutableParameters{
			Version:         2,
			MaxPackSize:     maxPackSize,
			IndexVersion:    index.Version2,
			EpochParameters: epoch.DefaultParameters(),
		},
	})
}

// TestCommitSessionToleratesLockedSessionMarker verifies that with
// ManagerOptions.ObjectLockEnabled a flush succeeds even when the session
// marker blob cannot be deleted. This is the S3 Object Lock case: a bucket
// default retention locks every object, including the session markers kopia
// normally deletes at flush (session markers are deliberately excluded from
// GetLockingStoragePrefixes, so kopia never requests a lock on them, but a
// bucket-level policy applies regardless). The just-written content must stay
// durable and readable, and the undeletable marker must simply be left in
// place - it expires with the retention the bucket applied.
func TestCommitSessionToleratesLockedSessionMarker(t *testing.T) {
	ctx := testlogging.Context(t)
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	faulty := blobtesting.NewFaultyStorage(st)

	bm, err := NewManagerForTesting(ctx, faulty, objectLockTestFormatProvider(t), nil, &ManagerOptions{ObjectLockEnabled: true})
	if err != nil {
		t.Fatalf("can't create content manager: %v", err)
	}

	defer bm.CloseShared(ctx)

	payload := seededRandomData(1, 100)

	cid, err := bm.WriteContent(ctx, gather.FromSlice(payload), "", NoCompression)
	if err != nil {
		t.Fatalf("WriteContent: %v", err)
	}

	// From here on every DeleteBlob fails, standing in for a retention-locked
	// session marker that S3 Object Lock (compliance mode) refuses to delete.
	faulty.AddFault(blobtesting.MethodDeleteBlob).Repeat(1000).ErrorInstead(errors.New("AccessDenied: object is WORM protected"))

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("flush must tolerate an undeletable session marker when object lock is enabled, got: %v", err)
	}

	// content survived the flush and is readable.
	got, err := bm.GetContent(ctx, cid)
	if err != nil {
		t.Fatalf("GetContent after flush: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Fatalf("content mismatch after flush: got %d bytes, want %d", len(got), len(payload))
	}

	// the marker that could not be deleted was left in storage rather than
	// aborting the flush.
	markers, err := blob.ListAllBlobs(ctx, st, BlobIDPrefixSession)
	if err != nil {
		t.Fatalf("listing session markers: %v", err)
	}

	if len(markers) == 0 {
		t.Fatal("expected the undeletable session marker to remain in storage")
	}
}

// TestCommitSessionFailsOnLockedSessionMarkerWithoutObjectLock is the negative
// case: without ObjectLockEnabled a DeleteBlob failure on the session marker
// stays fatal to the flush, preserving the strict behavior for non-object-lock
// repositories, where a delete failure signals a real storage problem.
func TestCommitSessionFailsOnLockedSessionMarkerWithoutObjectLock(t *testing.T) {
	ctx := testlogging.Context(t)
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	faulty := blobtesting.NewFaultyStorage(st)

	bm, err := NewManagerForTesting(ctx, faulty, objectLockTestFormatProvider(t), nil, &ManagerOptions{})
	if err != nil {
		t.Fatalf("can't create content manager: %v", err)
	}

	defer bm.CloseShared(ctx)

	if _, err := bm.WriteContent(ctx, gather.FromSlice(seededRandomData(1, 100)), "", NoCompression); err != nil {
		t.Fatalf("WriteContent: %v", err)
	}

	faulty.AddFault(blobtesting.MethodDeleteBlob).Repeat(1000).ErrorInstead(errors.New("AccessDenied"))

	if err := bm.Flush(ctx); err == nil {
		t.Fatal("flush must fail on an undeletable session marker when object lock is disabled")
	}
}
