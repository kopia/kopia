package content

import (
	"errors"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/format"
	"github.com/stretchr/testify/require"
)

// TestDeleteMarkerFlushTransientFailure verifies that deleting a committed
// content and hitting a transient storage failure during the subsequent
// Flush surfaces the storage error (instead of panicking on the
// packIndexBuilder invariant), and that retrying the flush after the
// transient failure persists the deletion marker: after reopening, the
// content is reported deleted while remaining readable (tombstone keeps
// the original pack location).
func TestDeleteMarkerFlushTransientFailure(t *testing.T) {
	ctx := testlogging.Context(t)

	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	faulty := blobtesting.NewFaultyStorage(st)

	s := &contentManagerSuite{mutableParameters: format.MutableParameters{
		Version:      1,
		IndexVersion: 1,
		MaxPackSize:  maxPackSize,
	}}

	bm := s.newTestContentManager(t, faulty)
	payload := seededRandomData(9, 100)

	id, err := bm.WriteContent(ctx, gather.FromSlice(payload), "", NoCompression)
	require.NoError(t, err)
	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	// reopen so the content only exists in the committed index.
	bm = s.newTestContentManager(t, faulty)

	require.NoError(t, bm.DeleteContent(ctx, id))

	errFlush := errors.New("transient put failure")
	faulty.AddFault(blobtesting.MethodPutBlob).ErrorInstead(errFlush)

	// transient failure must surface as an error, not a panic.
	require.ErrorIs(t, bm.Flush(ctx), errFlush)
	faulty.VerifyAllFaultsExercised(t)

	// with the fault consumed, the retry persists the deleted marker.
	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	// a fresh manager reflects the persisted deletion marker and the
	// deleted content remains readable (tombstone keeps pack location).
	bm = s.newTestContentManager(t, faulty)
	defer bm.CloseShared(ctx)

	ci, err := bm.ContentInfo(ctx, id)
	require.NoError(t, err)
	require.True(t, ci.Deleted)

	verifyContent(ctx, t, bm, id, payload)
}
