package compression

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

func init() {
	RegisterCompressor("zstd", newZstdCompressor(headerZstdDefault, zstd.SpeedDefault))
	RegisterCompressor("zstd-fastest", newZstdCompressor(headerZstdFastest, zstd.SpeedFastest))
	RegisterCompressor("zstd-better-compression", newZstdCompressor(headerZstdBetterCompression, zstd.SpeedBetterCompression))
	RegisterCompressor("zstd-best-compression", newZstdCompressor(headerZstdBestCompression, zstd.SpeedBestCompression))
}

func newZstdCompressor(id HeaderID, level zstd.EncoderLevel) Compressor {
	return &zstdCompressor{id, compressionHeader(id), level}
}

type zstdCompressor struct {
	id     HeaderID
	header []byte
	level  zstd.EncoderLevel
}

func (c *zstdCompressor) HeaderID() HeaderID {
	return c.id
}

func (c *zstdCompressor) Compress(b []byte) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(c.level))
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

func (c *zstdCompressor) Decompress(b []byte) ([]byte, error) {
	if len(b) < 4 {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:4], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r, err := zstd.NewReader(bytes.NewReader(b[4:]))
	if err != nil {
		return nil, errors.Wrap(err, "unable to open zstd stream")
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
