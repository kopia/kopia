package compression

import (
	"io"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/freepool"
	"github.com/kopia/kopia/internal/iocopy"
)

const (
	s2Parallel4Concurrency = 4
	s2Parallel8Concurrency = 8
)

func init() {
	RegisterCompressor("s2-default", newS2Compressor(headerS2Default))
	RegisterCompressor("s2-better", newS2Compressor(headerS2Better, s2.WriterBetterCompression()))
	RegisterCompressor("s2-parallel-4", newS2Compressor(headerS2Parallel4, s2.WriterConcurrency(s2Parallel4Concurrency)))
	RegisterCompressor("s2-parallel-8", newS2Compressor(headerS2Parallel8, s2.WriterConcurrency(s2Parallel8Concurrency)))
}

func newS2Compressor(id HeaderID, opts ...s2.WriterOption) Compressor {
	return &s2Compressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			return s2.NewWriter(io.Discard, opts...)
		},
	}}
}

type s2Compressor struct {
	id     HeaderID
	header []byte
	pool   sync.Pool
}

func (c *s2Compressor) HeaderID() HeaderID {
	return c.id
}

func (c *s2Compressor) Compress(output io.Writer, input io.Reader) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	//nolint:forcetypeassert
	w := c.pool.Get().(*s2.Writer)
	defer c.pool.Put(w)

	w.Reset(output)

	if err := iocopy.JustCopy(w, input); err != nil {
		return errors.Wrap(err, "compression error")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "compression close error")
	}

	return nil
}

//nolint:gochecknoglobals
var s2DecoderPool = freepool.New(func() *s2.Reader {
	return s2.NewReader(nil)
}, func(v *s2.Reader) {
	v.Reset(nil)
})

func (c *s2Compressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	if withHeader {
		if err := verifyCompressionHeader(input, c.header); err != nil {
			return err
		}
	}

	dec := s2DecoderPool.Take()
	defer s2DecoderPool.Return(dec)

	dec.Reset(input)

	if err := iocopy.JustCopy(output, dec); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
