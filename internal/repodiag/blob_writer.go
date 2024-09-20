package repodiag

import (
	"context"
	"sync"

	"github.com/kopia/kopia/internal/blobcrypto"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("repodiag")

// BlobWriter manages encryption and asynchronous writing of diagnostic blobs to the repository.
type BlobWriter struct {
	st blob.Storage
	bc blobcrypto.Crypter
	wg sync.WaitGroup
}

// EncryptAndWriteBlobAsync encrypts given content and writes it to the repository asynchronously,
// folllowed by calling the provided closeFunc.
func (w *BlobWriter) EncryptAndWriteBlobAsync(ctx context.Context, prefix blob.ID, data gather.Bytes, closeFunc func()) {
	encrypted := gather.NewWriteBuffer()
	// Close happens in a goroutine

	blobID, err := blobcrypto.Encrypt(w.bc, data, prefix, "", encrypted)
	if err != nil {
		encrypted.Close()

		// this should not happen, also nothing can be done about this, we're not in a place where we can return error, log it.
		log(ctx).Warnf("unable to encrypt diagnostics blob: %v", err)

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
			log(ctx).Warnf("unable to write diagnostics blob: %v", err)
			return
		}
	}()
}

// Wait waits for all the writes to complete.
func (w *BlobWriter) Wait(ctx context.Context) error {
	w.wg.Wait()
	return nil
}

// NewWriter creates a new writer.
func NewWriter(
	st blob.Storage,
	bc blobcrypto.Crypter,
) *BlobWriter {
	return &BlobWriter{st: st, bc: bc}
}
