//go:build linux || freebsd || darwin
// +build linux freebsd darwin

// Package stat provides a cross-platform abstraction for
// common stat commands.
package stat

import (
	"syscall"

	"github.com/pkg/errors"
)

const (
	diskBlockSize uint64 = 512
)

var errInvalidBlockSize = errors.New("invalid disk block size")

// GetFileAllocSize gets the space allocated on disk for the file
// 'fname' in bytes.
func GetFileAllocSize(fname string) (uint64, error) {
	var st syscall.Stat_t

	err := syscall.Stat(fname, &st)
	if err != nil {
		return 0, err //nolint:wrapcheck
	}

	//nolint:gosec
	return uint64(st.Blocks) * diskBlockSize, nil
}

// GetBlockSize gets the disk block size of the underlying system.
func GetBlockSize(path string) (uint64, error) {
	var st syscall.Statfs_t

	err := syscall.Statfs(path, &st)
	if err != nil {
		return 0, err //nolint:wrapcheck
	}

	if st.Bsize <= 0 {
		return 0, errors.Wrapf(errInvalidBlockSize, "%d", st.Bsize)
	}

	return uint64(st.Bsize), nil //nolint:unconvert,nolintlint
}
