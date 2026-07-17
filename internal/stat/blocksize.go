package stat

import "github.com/pkg/errors"

var errInvalidParameter = errors.New("invalid parameter")

// GetBlockAlignedSize returns the block-aligned `size` for blocks of
// blockSizeBytes. The size parameter must be non-negative and blockSizeBytes
// must be greater than zero. It returns an error when the parameters are invalid
// or the return value overflows an int64.
func GetBlockAlignedSize(size, blockSizeBytes int64) (int64, error) {
	if blockSizeBytes <= 0 {
		return -1, errors.Wrap(errInvalidParameter, "blockSizeBytes must be positive")
	}

	if size == 0 {
		return 0, nil
	}

	if size < 0 {
		return -1, errors.Wrap(errInvalidParameter, "blockSizeBytes must be non-negative")
	}

	if s := ((size-1)/blockSizeBytes + 1) * blockSizeBytes; s > 0 {
		return s, nil
	}

	return -1, errors.Wrap(errInvalidParameter, "aligned size overflows int64")
}
