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

func (c *s2Compressor) Compress(output *bytes.Buffer, input []byte) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*s2.Writer)
	defer c.pool.Put(w)

	w.Reset(output)

	if _, err := w.Write(input); err != nil {
		return errors.Wrap(err, "compression error")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "compression close error")
	}

	return nil
}

func (c *s2Compressor) Decompress(output *bytes.Buffer, input []byte) error {
	if len(input) < compressionHeaderSize {
		return errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(input[0:compressionHeaderSize], c.header) {
		return errors.Errorf("invalid compression header")
	}

	r := s2.NewReader(bytes.NewReader(input[compressionHeaderSize:]))

	if _, err := iocopy.Copy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
