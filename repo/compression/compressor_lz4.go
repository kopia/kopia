package compression

import (
	"errors"
	"io"
)

func init() {
	RegisterDeprecatedCompressor("lz4", lz4Compressor{})
}

var errLZ4NotSupported = errors.New("LZ4 compressor is not supported in recent versions of kopia, version v0.22.3 or older is needed to read legacy repositories that use the LZ4 compressor")

type lz4Compressor struct{}

func (c lz4Compressor) HeaderID() HeaderID {
	return headerLZ4Removed
}

func (c lz4Compressor) Compress(_ io.Writer, _ io.Reader) error {
	return errLZ4NotSupported
}

func (c lz4Compressor) Decompress(_ io.Writer, _ io.Reader, _ bool) error {
	return errLZ4NotSupported
}
