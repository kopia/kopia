package compression

import (
	"bytes"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("s2-default", newS2Compressor(headerS2Default))
	RegisterCompressor("s2-better", newS2Compressor(headerS2Better, s2.WriterBetterCompression()))
	RegisterCompressor("s2-parallel-4", newS2Compressor(headerS2Parallel4, s2.WriterConcurrency(4))) //nolint:gomnd
	RegisterCompressor("s2-parallel-8", newS2Compressor(headerS2Parallel8, s2.WriterConcurrency(8))) //nolint:gomnd
}

func newS2Compressor(id HeaderID, opts ...s2.WriterOption) Compressor {
	return &s2Compressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			return s2.NewWriter(bytes.NewBuffer(nil), opts...)
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

func (c *s2Compressor) Compress(output, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(output[:0])

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*s2.Writer)
	defer c.pool.Put(w)

	w.Reset(buf)

	if _, err := w.Write(b); err != nil {
		return nil, errors.Wrap(err, "compression error")
	}

	if err := w.Close(); err != nil {
		return nil, errors.Wrap(err, "compression close error")
	}

	return buf.Bytes(), nil
}

func (c *s2Compressor) Decompress(output, b []byte) ([]byte, error) {
	if len(b) < compressionHeaderSize {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:compressionHeaderSize], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r := s2.NewReader(bytes.NewReader(b[compressionHeaderSize:]))

	buf := bytes.NewBuffer(output[:0])
	if _, err := iocopy.Copy(buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
