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
	return &zstdCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, err := zstd.NewWriter(bytes.NewBuffer(nil), zstd.WithEncoderLevel(level))
			mustSucceed(err)
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

func (c *zstdCompressor) Compress(output *bytes.Buffer, input []byte) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	w := c.pool.Get().(*zstd.Encoder)
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

func (c *zstdCompressor) Decompress(output *bytes.Buffer, input []byte) error {
	if len(input) < compressionHeaderSize {
		return errors.Errorf("invalid compression header")
	}

	if !bytes.Equal(input[0:compressionHeaderSize], c.header) {
		return errors.Errorf("invalid compression header")
	}

	r, err := zstd.NewReader(bytes.NewReader(input[compressionHeaderSize:]))
	if err != nil {
		return errors.Wrap(err, "unable to open zstd stream")
	}
	defer r.Close()

	if _, err := iocopy.Copy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
