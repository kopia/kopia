package compression

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/freepool"
	"github.com/kopia/kopia/internal/iocopy"
)

func init() {
	RegisterCompressor("zstd", newZstdCompressor(HeaderZstdDefault, zstd.SpeedDefault))
	RegisterCompressor("zstd-fastest", newZstdCompressor(HeaderZstdFastest, zstd.SpeedFastest))
	RegisterCompressor("zstd-better-compression", newZstdCompressor(HeaderZstdBetterCompression, zstd.SpeedBetterCompression))
	RegisterDeprecatedCompressor("zstd-best-compression", newZstdCompressor(HeaderZstdBestCompression, zstd.SpeedBestCompression))
}

func newZstdCompressor(id HeaderID, level zstd.EncoderLevel) Compressor {
	return &zstdCompressor{id, compressionHeader(id), sync.Pool{
		New: func() interface{} {
			w, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(level))
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

func (c *zstdCompressor) Compress(output io.Writer, input io.Reader) error {
	if _, err := output.Write(c.header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	//nolint:forcetypeassert
	w := c.pool.Get().(*zstd.Encoder)
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
var zstdDecoderPool = freepool.New(func() *zstd.Decoder {
	r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	mustSucceed(err)
	return r
}, func(v *zstd.Decoder) {
	mustSucceed(v.Reset(nil))
})

func (c *zstdCompressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	if withHeader {
		if err := verifyCompressionHeader(input, c.header); err != nil {
			return err
		}
	}

	dec := zstdDecoderPool.Take()
	defer zstdDecoderPool.Return(dec)

	if err := dec.Reset(input); err != nil {
		return errors.Wrap(err, "decompression reset error")
	}

	if err := iocopy.JustCopy(output, dec); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
