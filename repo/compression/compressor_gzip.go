package compression

import (
	"bytes"
	"compress/gzip"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("gzip", newGZipCompressor(headerGzipDefault, gzip.DefaultCompression))
	RegisterCompressor("gzip-best-speed", newGZipCompressor(headerGzipBestSpeed, gzip.BestSpeed))
	RegisterCompressor("gzip-best-compression", newGZipCompressor(headerGzipBestCompression, gzip.BestCompression))
}

func newGZipCompressor(id HeaderID, level int) Compressor {
	// check that this works, we'll be using this without possibility of returning error below
	if _, err := gzip.NewWriterLevel(bytes.NewBuffer(nil), level); err != nil {
		panic("unexpected failure when creting writer")
	}

	return &gzipCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, _ := gzip.NewWriterLevel(bytes.NewBuffer(nil), level)
			return w
		},
	}}
}

type gzipCompressor struct {
	id     HeaderID
	header []byte
	pool   sync.Pool
}

func (c *gzipCompressor) HeaderID() HeaderID {
	return c.id
}

func (c *gzipCompressor) Compress(output, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(output[:0])

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*gzip.Writer)
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

func (c *gzipCompressor) Decompress(output, b []byte) ([]byte, error) {
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

	buf := bytes.NewBuffer(output[:0])
	if _, err := iocopy.Copy(buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
