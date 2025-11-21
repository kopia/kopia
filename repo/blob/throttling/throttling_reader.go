package throttling

import (
	"context"
	"io"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo/blob"
)

// ThrottledBytes acts as blob.Bytes but ensures that reads are throttled.
type ThrottledBytes struct {
	data      blob.Bytes
	throttler Throttler
	ctx       context.Context //nolint:containedctx
}

var _ blob.Bytes = &ThrottledBytes{} // check we meet the interface we're trying to meet

// Length returns the length.
func (t ThrottledBytes) Length() int {
	return t.data.Length()
}

// WriteTo writes to the writer while meeting the rate limit.
func (t ThrottledBytes) WriteTo(w io.Writer) (int64, error) {
	// Use Copy & t.Reader rather than delegate to t.data.WriteTo to ensure we rate limit
	return iocopy.Copy(w, t.Reader()) //nolint:wrapcheck
}

// Reader returns a throttled reader.
func (t ThrottledBytes) Reader() io.ReadSeekCloser {
	return NewReader(t.ctx, t.data.Reader(), t.throttler)
}

// ThrottledReader acts as io.ReadSeekCloser but ensures that reads are throttled.
type ThrottledReader struct {
	r         io.ReadSeekCloser
	throttler Throttler
	ctx       context.Context //nolint:containedctx
}

var _ io.ReadSeekCloser = &ThrottledReader{} // check we meet the interface we're trying to meet

// NewReader returns a reader that implements io.ReadSeekCloser with rate limiting.
func NewReader(ctx context.Context, r io.ReadSeekCloser, t Throttler) *ThrottledReader {
	return &ThrottledReader{
		r:         r,
		throttler: t,
		ctx:       ctx,
	}
}

// Read reads.
func (s *ThrottledReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil {
		//nolint:wrapcheck
		return n, err
	}

	s.throttler.DuringUpload(s.ctx, int64(n))

	return n, nil
}

// Seek seeks.
func (s *ThrottledReader) Seek(offset int64, whence int) (int64, error) {
	//nolint:wrapcheck
	return s.r.Seek(offset, whence)
}

// Close closes.
func (s *ThrottledReader) Close() error {
	//nolint:wrapcheck
	return s.r.Close()
}
