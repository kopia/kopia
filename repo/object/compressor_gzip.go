package object

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/pkg/errors"
)

func init() {
	RegisterCompressor("gzip", newGZipCompressor(0x1000, gzip.DefaultCompression))
	RegisterCompressor("gzip-best-speed", newGZipCompressor(0x1001, gzip.BestSpeed))
	RegisterCompressor("gzip-best-compression", newGZipCompressor(0x1002, gzip.BestCompression))
}

func newGZipCompressor(id uint32, level int) Compressor {
	return &gzipCompressor{id, compressionHeader(id), level}
}

type gzipCompressor struct {
	id     uint32
	header []byte
	level  int
}

func (c *gzipCompressor) ID() uint32 {
	return c.id
}

func (c *gzipCompressor) Compress(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w, err := gzip.NewWriterLevel(&buf, c.level)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create compressor")
	}

	if _, err := w.Write(b); err != nil {
		return nil, errors.Wrap(err, "compression error")
	}

	if err := w.Close(); err != nil {
		return nil, errors.Wrap(err, "compression close error")
	}

	return buf.Bytes(), nil
}

func (c *gzipCompressor) Decompress(b []byte) ([]byte, error) {
	if len(b) < 4 {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:4], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r, err := gzip.NewReader(bytes.NewReader(b[4:]))
	if err != nil {
		return nil, errors.Wrap(err, "unable to open gzip stream")
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
