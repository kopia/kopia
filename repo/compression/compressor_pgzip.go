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
	// check that this works, we'll be using this without possibility of returning error below
	if _, err := pgzip.NewWriterLevel(bytes.NewBuffer(nil), level); err != nil {
		panic("unexpected failure when creting writer")
	}

	return &pgzipCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, _ := pgzip.NewWriterLevel(bytes.NewBuffer(nil), level)
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

func (c *pgzipCompressor) Compress(output, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(output[:0])

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*pgzip.Writer)
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

func (c *pgzipCompressor) Decompress(output, b []byte) ([]byte, error) {
	if len(b) < compressionHeaderSize {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:compressionHeaderSize], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r, err := pgzip.NewReader(bytes.NewReader(b[compressionHeaderSize:]))
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
