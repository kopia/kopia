//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package robustness

import (
	"errors"
)

var (
	// ErrNoOp is thrown when an action could not do anything useful.
	ErrNoOp = errors.New("no-op")

	// ErrCannotPerformIO is returned if the engine determines there is not enough space
	// to write files.
	ErrCannotPerformIO = errors.New("cannot perform i/o")

	// ErrNoActionPicked is returned if a random action could not be selected.
	ErrNoActionPicked = errors.New("unable to pick an action with the action control options provided")

	// ErrInvalidOption is returned if an option value is invalid or missing.
	ErrInvalidOption = errors.New("invalid option setting")

	// ErrKeyNotFound is returned when the store can't find the key provided.
	ErrKeyNotFound = errors.New("key not found")

	// ErrMetadataMissing is returned when the metadata can't be found.
	ErrMetadataMissing = errors.New("metadata missing")
)
