package compression

import (
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"

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
var zstdDecoderOptions []zstd.DOption

func init() {
	if v, err := strconv.Atoi(os.Getenv("KOPIA_ZSTD_DECODER_CONCURRENCY")); err == nil {
		zstdDecoderOptions = append(zstdDecoderOptions, zstd.WithDecoderConcurrency(v))
	}

	if v, err := strconv.Atoi(os.Getenv("KOPIA_ZSTD_DECODER_MAX_MEMORY")); err == nil {
		zstdDecoderOptions = append(zstdDecoderOptions, zstd.WithDecoderMaxMemory(uint64(v)))
	}

	if v, err := strconv.Atoi(os.Getenv("KOPIA_ZSTD_DECODER_MAX_WINDOW")); err == nil {
		zstdDecoderOptions = append(zstdDecoderOptions, zstd.WithDecoderMaxWindow(uint64(v)))
	}

	if v, err := strconv.Atoi(os.Getenv("KOPIA_ZSTD_DECODER_LOWMEM")); err == nil {
		zstdDecoderOptions = append(zstdDecoderOptions, zstd.WithDecoderLowmem(v != 0))
	}
}

func (c *zstdCompressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	if withHeader {
		if err := verifyCompressionHeader(input, c.header); err != nil {
			return err
		}
	}

	r, err := zstd.NewReader(input, zstdDecoderOptions...)
	if err != nil {
		return errors.Wrap(err, "unable to open zstd stream")
	}
	defer r.Close()

	if err := iocopy.JustCopy(output, r); err != nil {
		return errors.Wrap(err, "decompression error")
	}

	return nil
}
