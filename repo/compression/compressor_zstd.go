package compression

import (
	"bytes"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("zstd", newZstdCompressor(headerZstdDefault, zstd.SpeedDefault))
	RegisterCompressor("zstd-fastest", newZstdCompressor(headerZstdFastest, zstd.SpeedFastest))
	RegisterCompressor("zstd-better-compression", newZstdCompressor(headerZstdBetterCompression, zstd.SpeedBetterCompression))
	RegisterCompressor("zstd-best-compression", newZstdCompressor(headerZstdBestCompression, zstd.SpeedBestCompression))
}

func newZstdCompressor(id HeaderID, level zstd.EncoderLevel) Compressor {
	// check that this works, we'll be using this without possibility of returning error below
	if _, err := zstd.NewWriter(bytes.NewBuffer(nil), zstd.WithEncoderLevel(level)); err != nil {
		panic("unexpected failure when creting writer")
	}

	return &zstdCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, _ := zstd.NewWriter(bytes.NewBuffer(nil), zstd.WithEncoderLevel(level))
			return w
		},
	}}
}

type zstdCompressor struct {
	id     HeaderID
	header []byte
	pool   sync.Pool
}

func (c *zstdCompressor) HeaderID() HeaderID {
	return c.id
}

func (c *zstdCompressor) Compress(output, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(output[:0])

	if _, err := buf.Write(c.header); err != nil {
		return nil, errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*zstd.Encoder)
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

func (c *zstdCompressor) Decompress(output, b []byte) ([]byte, error) {
	if len(b) < compressionHeaderSize {
		return nil, errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(b[0:compressionHeaderSize], c.header) {
		return nil, errors.Errorf("invalid compression header")
	}

	r, err := zstd.NewReader(bytes.NewReader(b[compressionHeaderSize:]))
	if err != nil {
		return nil, errors.Wrap(err, "unable to open zstd stream")
	}
	defer r.Close()

	buf := bytes.NewBuffer(output[:0])
	if _, err := iocopy.Copy(buf, r); err != nil {
		return nil, errors.Wrap(err, "decompression error")
	}

	return buf.Bytes(), nil
}
