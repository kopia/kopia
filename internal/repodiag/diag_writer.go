package repodiag

import (
	"context"
	"sync"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// Writer manages encryption and asynchronous writing of diagnostic blobs to the repository.
type Writer struct {
	st blob.Storage
	bc blobcrypto.Crypter
	wg sync.WaitGroup
}

func (w *Writer) encryptAndWriteLogBlobAsync(ctx context.Context, prefix blob.ID, data gather.Bytes, closeFunc func()) {
	encrypted := gather.NewWriteBuffer()
	// Close happens in a goroutine

	blobID, err := blobcrypto.Encrypt(w.bc, data, prefix, "", encrypted)
	if err != nil {
		encrypted.Close()

		// this should not happen, also nothing can be done about this, we're not in a place where we can return error, log it.
		return
	}

	b := encrypted.Bytes()

	w.wg.Add(1)

	go func() {
		defer w.wg.Done()
		defer encrypted.Close()
		defer closeFunc()

		if err := w.st.PutBlob(ctx, blobID, b, blob.PutOptions{}); err != nil {
			// nothing can be done about this, we're not in a place where we can return error, log it.
			return
		}
	}()
}

// Close closes the diagnostics writer.
func (w *Writer) Close(ctx context.Context) error {
	w.wg.Wait()
	return nil
}

// NewWriter creates a new writer.
func NewWriter(
	st blob.Storage,
	bc blobcrypto.Crypter,
) *Writer {
	return &Writer{st: st, bc: bc}
}
