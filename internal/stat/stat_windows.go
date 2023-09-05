//go:build windows
// +build windows

// Package stat provides a cross-platform abstraction for
// common stat commands.
package stat

import "errors"

var errNotImplemented = errors.New("not implemented")

// GetFileAllocSize gets the space allocated on disk for the file
// 'fname' in bytes.
//
//nolint:revive
func GetFileAllocSize(fname string) (uint64, error) {
	return 0, errNotImplemented
}

// GetBlockSize gets the disk block size of the underlying system.
//
//nolint:revive
func GetBlockSize(path string) (uint64, error) {
	return 0, errNotImplemented
}
