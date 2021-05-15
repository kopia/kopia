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
	return &gzipCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, err := gzip.NewWriterLevel(bytes.NewBuffer(nil), level)
			mustSucceed(err)
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

func (c *gzipCompressor) Compress(output *bytes.Buffer, input []byte) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	// nolint:forcetypeassert
	w := c.pool.Get().(*gzip.Writer)
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

func (c *gzipCompressor) Decompress(output *bytes.Buffer, b []byte) error {
	if err := verifyCompressionHeader(b, c.header); err != nil {
		return err
	}

	r, err := gzip.NewReader(bytes.NewReader(b[compressionHeaderSize:]))
	if err != nil {
		return errors.Wrap(err, "unable to open gzip stream")
	}
	defer r.Close() //nolint:errcheck

	if _, err := iocopy.Copy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
