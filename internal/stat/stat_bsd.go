//go:build openbsd
// +build openbsd

// Package stat provides a cross-platform abstraction for
// common stat commands.
package stat

import "syscall"

const (
	diskBlockSize uint64 = 512
)

// GetFileAllocSize gets the space allocated on disk for the file.
// 'fname' in bytes.
func GetFileAllocSize(fname string) (uint64, error) {
	var st syscall.Stat_t

	err := syscall.Stat(fname, &st)
	if err != nil {
		return 0, err // nolint:wrapcheck
	}

	return uint64(st.Blocks) * diskBlockSize, nil
}

// GetBlockSize gets the disk block size of the underlying system.
func GetBlockSize(path string) (uint64, error) {
	var st syscall.Statfs_t

	err := syscall.Statfs(path, &st)
	if err != nil {
		return 0, err // nolint:wrapcheck
	}

	return uint64(st.F_bsize), nil
}
