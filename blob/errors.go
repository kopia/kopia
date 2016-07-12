package blob

import "errors"

var (
	// ErrBlockNotFound is returned when a block cannot be found in blob.
	ErrBlockNotFound = errors.New("block not found")

	// ErrInvalidChecksum is returned when a storage block is invalid, which may indicate
	// that decryption has failed.
	ErrInvalidChecksum = errors.New("invalid checksum")
)
