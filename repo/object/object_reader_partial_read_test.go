package object

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/content"
	"github.com/stretchr/testify/require"
)

//nolint:gochecknoglobals
var errInjectedChunkFault = errors.New("injected targeted chunk fault")

// targetedFaultContentManager embeds *fakeContentManager and fails GetContent
// for exactly one content.ID while faultErr is set.
type targetedFaultContentManager struct {
	*fakeContentManager
	faultContentID content.ID
	faultErr       error
}

func (f *targetedFaultContentManager) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	if f.faultErr != nil && contentID == f.faultContentID {
		return nil, f.faultErr
	}

	return f.fakeContentManager.GetContent(ctx, contentID)
}

// TestObjectReaderReturnsPartialDataOnChunkError verifies the io.Reader
// contract of objectReader.Read: when a read spanning multiple chunks has
// already copied bytes from the first chunk into the caller's buffer and the
// NEXT chunk fails to load, Read must return (n > 0, err) with the copied
// bytes — not (0, err), which silently discards them while the reader
// position advances past the loss.
func TestObjectReaderReturnsPartialDataOnChunkError(t *testing.T) {
	ctx := testlogging.Context(t)

	data, fcm, om := setupTest(t, nil)

	payload := make([]byte, 2<<20+4096)
	_, err := rand.Read(payload)
	require.NoError(t, err)

	w := om.NewWriter(ctx, WriterOptions{})
	_, err = w.Write(payload)
	require.NoError(t, err)

	oid, err := w.Result()
	require.NoError(t, err)
	w.Close()

	ndx, ok := oid.IndexObjectID()
	require.True(t, ok, "object over 2 chunks must be indirect")

	entries, err := LoadIndexObject(ctx, fcm, ndx)
	require.NoError(t, err)
	require.Len(t, entries, 3, "FIXED-1M must split the payload into 3 chunks")

	targetCID, _, ok := entries[1].Object.ContentID()
	require.True(t, ok)
	require.Contains(t, data, targetCID, "target chunk content must exist before the fault")

	wcm := &targetedFaultContentManager{fakeContentManager: fcm, faultContentID: targetCID, faultErr: errInjectedChunkFault}

	r, err := Open(ctx, wcm, oid)
	require.NoError(t, err, "Open must not need to read chunk data")

	defer r.Close() //nolint:errcheck

	require.Equal(t, int64(len(payload)), r.Length())

	// single read large enough to span the chunk boundary: it must copy all
	// of chunk 0 and then fail while opening chunk 1.
	buf := make([]byte, len(payload)+64)

	n, rerr := r.Read(buf)

	require.ErrorIs(t, rerr, errInjectedChunkFault)
	require.Positive(t, n, "Read must return the bytes already copied before the chunk error (io.Reader contract)")
	require.Equal(t, int(entries[0].Length), n, "all of chunk 0 must have been copied before the failure")
	require.True(t, bytes.Equal(buf[:n], payload[:n]), "returned bytes must be a genuine payload prefix")

	// after the fault is cleared, a fresh reader must still serve the whole
	// object — proving no data was corrupted, only the single-call semantics
	// were at stake.
	wcm.faultErr = nil

	r2, err := Open(ctx, wcm, oid)
	require.NoError(t, err)

	defer r2.Close() //nolint:errcheck

	got, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, payload, got, "post-recovery full read must return the original payload intact")
}
