package compression

import (
	"bytes"
	"compress/gzip"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("gzip", newGZipCompressor(headerGzipDefault, gzip.DefaultCompression))
	RegisterCompressor("gzip-best-speed", newGZipCompressor(headerGzipBestSpeed, gzip.BestSpeed))
	RegisterCompressor("gzip-best-compression", newGZipCompressor(headerGzipBestCompression, gzip.BestCompression))
}

func newGZipCompressor(id HeaderID, level int) Compressor {
	return &gzipCompressor{id, compressionHeader(id), level}
}

type gzipCompressor struct {
	id     HeaderID
	header []byte
	level  int
}

func (c *gzipCompressor) HeaderID() HeaderID {
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
	if len(b) < compressionHeaderSize {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:compressionHeaderSize], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r, err := gzip.NewReader(bytes.NewReader(b[compressionHeaderSize:]))
	if err != nil {
		return nil, errors.Wrap(err, "unable to open gzip stream")
	}
	defer r.Close() //nolint:errcheck

	var buf bytes.Buffer
	if _, err := iocopy.Copy(&buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
