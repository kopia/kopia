package compression

import (
	"compress/gzip"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/freepool"
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
			w, err := gzip.NewWriterLevel(io.Discard, level)
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

func (c *gzipCompressor) Compress(output io.Writer, input io.Reader) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	//nolint:forcetypeassert
	w := c.pool.Get().(*gzip.Writer)
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
var gzipDecoderPool = freepool.New(func() *gzip.Reader {
	return new(gzip.Reader)
}, func(_ *gzip.Reader) {})

func (c *gzipCompressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	if withHeader {
		if err := verifyCompressionHeader(input, c.header); err != nil {
			return err
		}
	}

	dec := gzipDecoderPool.Take()
	defer gzipDecoderPool.Return(dec)

	mustSucceed(dec.Reset(input))

	if err := iocopy.JustCopy(output, dec); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
