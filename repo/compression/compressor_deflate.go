package compression

import (
	"bytes"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("deflate-best-speed", newDeflateCompressor(headerDeflateBestSpeed, flate.BestSpeed))
	RegisterCompressor("deflate-default", newDeflateCompressor(headerDeflateDefault, flate.DefaultCompression))
	RegisterCompressor("deflate-best-compression", newDeflateCompressor(headerDeflateBestCompression, flate.BestCompression))
}

func newDeflateCompressor(id HeaderID, level int) Compressor {
	return &deflateCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			v, err := flate.NewWriter(bytes.NewBuffer(nil), level)
			if err != nil {
				panic("unable to create deflate compressor")
			}

			return v
		},
	}}
}

type deflateCompressor struct {
	id     HeaderID
	header []byte
	pool   sync.Pool
}

func (c *deflateCompressor) HeaderID() HeaderID {
	return c.id
}

func (c *deflateCompressor) Compress(output *bytes.Buffer, input []byte) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	// nolint:forcetypeassert
	w := c.pool.Get().(*flate.Writer)
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

func (c *deflateCompressor) Decompress(output *bytes.Buffer, input []byte) error {
	if err := verifyCompressionHeader(input, c.header); err != nil {
		return err
	}

	r := flate.NewReader(bytes.NewReader(input[compressionHeaderSize:]))

	if _, err := iocopy.Copy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
