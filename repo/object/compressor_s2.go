package object

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
)

func init() {
	RegisterCompressor("s2-default", newS2Compressor(0x1200))
	RegisterCompressor("s2-better", newS2Compressor(0x1201, s2.WriterBetterCompression()))
	RegisterCompressor("s2-parallel-4", newS2Compressor(0x1202, s2.WriterConcurrency(4)))
	RegisterCompressor("s2-parallel-8", newS2Compressor(0x1203, s2.WriterConcurrency(8)))
}

func newS2Compressor(id uint32, opts ...s2.WriterOption) Compressor {
	return &s2Compressor{id, compressionHeader(id), opts}
}

type s2Compressor struct {
	id     uint32
	header []byte
	opts   []s2.WriterOption
}

func (c *s2Compressor) ID() uint32 {
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
	if len(b) < 4 {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:4], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r := s2.NewReader(bytes.NewReader(b[4:]))

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
