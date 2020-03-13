package compression

import (
	"bytes"
	"sync"

	"github.com/klauspost/pgzip"
	"github.com/pkg/errors"

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

func (c *pgzipCompressor) Compress(output *bytes.Buffer, input []byte) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*pgzip.Writer)
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

func (c *pgzipCompressor) Decompress(output *bytes.Buffer, input []byte) error {
	if len(input) < compressionHeaderSize {
		return errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(input[0:compressionHeaderSize], c.header) {
		return errors.Errorf("invalid compression header")
	}

	r, err := pgzip.NewReader(bytes.NewReader(input[compressionHeaderSize:]))
	if err != nil {
		return errors.Wrap(err, "unable to open gzip stream")
	}
	defer r.Close() //nolint:errcheck

	if _, err := iocopy.Copy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
