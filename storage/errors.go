package storage

import "errors"

var (
	// ErrBlockNotFound is returned when a block cannot be found in repository.
	ErrBlockNotFound = errors.New("block not found")

	// ErrInvalidChecksum is returned when a repository block is invalid, which may indicate
	// that decryption has failed.
	ErrInvalidChecksum = errors.New("invalid checksum")

	// ErrWriteLimitExceeded is returned when the maximum amount of data has already been written
	// to the repository.
	ErrWriteLimitExceeded = errors.New("write limit exceeded")
)
