package compression

import (
	"bytes"
	"sync"

	"github.com/pierrec/lz4"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("lz4", newLZ4Compressor(headerLZ4Default))
}

func newLZ4Compressor(id HeaderID) Compressor {
	return &lz4Compressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(bytes.NewBuffer(nil))
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

func (c *lz4Compressor) Compress(output *bytes.Buffer, input []byte) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	// nolint:forcetypeassert
	w := c.pool.Get().(*lz4.Writer)
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

func (c *lz4Compressor) Decompress(output *bytes.Buffer, input []byte) error {
	if err := verifyCompressionHeader(input, c.header); err != nil {
		return err
	}

	r := lz4.NewReader(bytes.NewReader(input[compressionHeaderSize:]))

	if _, err := iocopy.Copy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
