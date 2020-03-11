package compression

import (
	"bytes"

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
	return &pgzipCompressor{id, compressionHeader(id), level}
}

type pgzipCompressor struct {
	id     HeaderID
	header []byte
	level  int
}

func (c *pgzipCompressor) HeaderID() HeaderID {
	return c.id
}

func (c *pgzipCompressor) Compress(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w, err := pgzip.NewWriterLevel(&buf, c.level)
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

func (c *pgzipCompressor) Decompress(b []byte) ([]byte, error) {
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

	var buf bytes.Buffer
	if _, err := iocopy.Copy(&buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
