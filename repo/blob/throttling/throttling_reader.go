package throttling

import (
	"context"
	"io"

	"github.com/kopia/kopia/repo/blob"
)

type ThrottledBytes struct {
	data      blob.Bytes
	throttler Throttler
	ctx       context.Context
}

var _ blob.Bytes = &ThrottledBytes{} // check we meet the interface we're trying to meet

func (t ThrottledBytes) Length() int {
	return t.data.Length()
}

func (t ThrottledBytes) WriteTo(w io.Writer) (int64, error) {
	// Use Copy rather than delegate to t.data.WriteTo to ensure we rate limit
	return io.Copy(w, t.Reader())
}

func (t ThrottledBytes) Reader() io.ReadSeekCloser {
	return NewReader(t.data.Reader(), t.throttler, t.ctx)
}

type ThrottledReader struct {
	r         io.ReadSeekCloser
	throttler Throttler
	ctx       context.Context
}

var _ io.ReadSeekCloser = &ThrottledReader{} // check we meet the interface we're trying to meet

// NewReader returns a reader that implements io.ReadSeekCloser with rate limiting
func NewReader(r io.ReadSeekCloser, t Throttler, ctx context.Context) *ThrottledReader {
	return &ThrottledReader{
		r:         r,
		throttler: t,
		ctx:       ctx,
	}
}

func (s *ThrottledReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil {
		return n, err
	}
	s.throttler.BeforeUpload(s.ctx, int64(n))
	return n, nil
}

func (s *ThrottledReader) Seek(offset int64, whence int) (int64, error) {
	return s.r.Seek(offset, whence)
}

func (s *ThrottledReader) Close() error {
	return s.r.Close()
}
