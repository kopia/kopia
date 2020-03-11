package compression

import (
	"bytes"

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
	return &s2Compressor{id, compressionHeader(id), opts}
}

type s2Compressor struct {
	id     HeaderID
	header []byte
	opts   []s2.WriterOption
}

func (c *s2Compressor) HeaderID() HeaderID {
	return c.id
}

func (c *s2Compressor) Compress(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w := s2.NewWriter(&buf, c.opts...)

	if _, err := w.Write(b); err != nil {
		return nil, errors.Wrap(err, "compression error")
	}

	if err := w.Close(); err != nil {
		return nil, errors.Wrap(err, "compression close error")
	}

	return buf.Bytes(), nil
}

func (c *s2Compressor) Decompress(b []byte) ([]byte, error) {
	if len(b) < compressionHeaderSize {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:compressionHeaderSize], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r := s2.NewReader(bytes.NewReader(b[compressionHeaderSize:]))

	var buf bytes.Buffer
	if _, err := iocopy.Copy(&buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
