package compression

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/pgzip"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/freepool"
	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("pgzip", newpgzipCompressor(headerPgzipDefault, pgzip.DefaultCompression))
	RegisterCompressor("pgzip-best-speed", newpgzipCompressor(headerPgzipBestSpeed, pgzip.BestSpeed))
	RegisterCompressor("pgzip-best-compression", newpgzipCompressor(headerPgzipBestCompression, pgzip.BestCompression))
}

func newpgzipCompressor(id HeaderID, level int) Compressor {
	return &pgzipCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, err := pgzip.NewWriterLevel(bytes.NewBuffer(nil), level)
			mustSucceed(err)
			return w
		},
	}}
}

type pgzipCompressor struct {
	id     HeaderID
	header []byte
	pool   sync.Pool
}

func (c *pgzipCompressor) HeaderID() HeaderID {
	return c.id
}

func (c *pgzipCompressor) Compress(output io.Writer, input io.Reader) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	//nolint:forcetypeassert
	w := c.pool.Get().(*pgzip.Writer)
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
var pgzipDecoderPool = freepool.New(func() *pgzip.Reader {
	return &pgzip.Reader{}
}, func(_ *pgzip.Reader) {})

func (c *pgzipCompressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	if withHeader {
		if err := verifyCompressionHeader(input, c.header); err != nil {
			return err
		}
	}

	dec := pgzipDecoderPool.Take()
	defer pgzipDecoderPool.Return(dec)

	mustSucceed(dec.Reset(input))

	if err := iocopy.JustCopy(output, dec); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
