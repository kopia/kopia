package storagereserve

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("storagereserve")

const (
	// ReserveBlobID is the name of the blob used for the storage reserve.
	ReserveBlobID = "kopia.reserve"

	// DefaultReserveSize is the default size of the reserve blob (500MB).
	DefaultReserveSize = 500 << 20
)

// Create creates the reserve blob in the provided storage.
func Create(ctx context.Context, st blob.Storage, size int64) error {
	log(ctx).Infof("Creating storage reserve of %v bytes...", size)

	data := &zeroBytes{length: int(size)}

	if err := st.PutBlob(ctx, ReserveBlobID, data, blob.PutOptions{}); err != nil {
		return errors.Wrap(err, "error creating reserve blob")
	}

	return nil
}

// Delete removes the reserve blob from the provided storage.
func Delete(ctx context.Context, st blob.Storage) error {
	log(ctx).Infof("Deleting storage reserve to free up space...")

	err := st.DeleteBlob(ctx, ReserveBlobID)
	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return errors.Wrap(err, "error deleting reserve blob")
}

// Exists checks if the reserve blob exists in the provided storage.
func Exists(ctx context.Context, st blob.Storage) (bool, error) {
	_, err := st.GetMetadata(ctx, ReserveBlobID)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, blob.ErrBlobNotFound) {
		return false, nil
	}

	return false, errors.Wrap(err, "error checking for reserve blob")
}

// Ensure ensures that the reserve blob exists in the provided storage.
// If it doesn't exist, it attempts to create it.
func Ensure(ctx context.Context, st blob.Storage, size int64) error {
	exists, err := Exists(ctx, st)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return Create(ctx, st, size)
}

// zeroBytes implements blob.Bytes by providing an infinite stream of zeros.
type zeroBytes struct {
	length int
}

func (b *zeroBytes) WriteTo(w io.Writer) (int64, error) {
	const bufSize = 64 << 10
	buf := make([]byte, bufSize)
	var total int64

	for total < int64(b.length) {
		toWrite := int64(bufSize)
		if remaining := int64(b.length) - total; remaining < toWrite {
			toWrite = remaining
		}

		n, err := w.Write(buf[:toWrite])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

func (b *zeroBytes) Length() int { return b.length }

func (b *zeroBytes) Reader() io.ReadSeekCloser {
	return &zeroReader{length: int64(b.length)}
}

type zeroReader struct {
	length int64
	offset int64
}

func (r *zeroReader) Read(p []byte) (n int, err error) {
	if r.offset >= r.length {
		return 0, io.EOF
	}

	remaining := r.length - r.offset
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	for i := range p {
		p[i] = 0
	}

	r.offset += int64(len(p))
	return len(p), nil
}

func (r *zeroReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.length + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if newOffset < 0 {
		return 0, errors.New("negative offset")
	}

	r.offset = newOffset
	return r.offset, nil
}

func (r *zeroReader) Close() error { return nil }
