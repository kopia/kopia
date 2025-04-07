package compression

import (
	"io"
	"sync"

	"github.com/pierrec/lz4"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/freepool"
	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterDeprecatedCompressor("lz4", newLZ4Compressor(headerLZ4Default))
}

func newLZ4Compressor(id HeaderID) Compressor {
	return &lz4Compressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(io.Discard)
		},
	}}
}

type lz4Compressor struct {
	id     HeaderID
	header []byte
	pool   sync.Pool
}

func (c *lz4Compressor) HeaderID() HeaderID {
	return c.id
}

func (c *lz4Compressor) Compress(output io.Writer, input io.Reader) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	//nolint:forcetypeassert
	w := c.pool.Get().(*lz4.Writer)
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
var lz4DecoderPool = freepool.New(func() *lz4.Reader {
	return lz4.NewReader(nil)
}, func(v *lz4.Reader) {
	v.Reset(nil)
})

func (c *lz4Compressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	if withHeader {
		if err := verifyCompressionHeader(input, c.header); err != nil {
			return err
		}
	}

	dec := lz4DecoderPool.Take()
	defer lz4DecoderPool.Return(dec)

	dec.Reset(input)

	if err := iocopy.JustCopy(output, dec); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
