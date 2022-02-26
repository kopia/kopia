//go:build windows
// +build windows

// Package stat provides a cross-platform abstraction for
// common stat commands.
package stat

import (
	"errors"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// GetFileAllocSize gets the space allocated on disk for the file
// 'fname' in bytes.
// nolint:gosec
func GetFileAllocSize(fname string) (uint64, error) {
	pathPtr, err := windows.UTF16PtrFromString(fname)
	if !errors.Is(err, syscall.Errno(0)) {
		return 0, err // nolint:wrapcheck
	}

	var size uint64

	GetCompressedFileSizeA := syscall.MustLoadDLL("kernel32.dll").MustFindProc("GetCompressedFileSizeA")

	_, _, err = GetCompressedFileSizeA.Call(
		uintptr(*pathPtr),
		uintptr(unsafe.Pointer(&size)))
	if !errors.Is(err, syscall.Errno(0)) {
		return 0, err //nolint:wrapcheck
	}

	return size, nil
}

// GetBlockSize gets the disk block size of the underlying system.
// nolint:gosec
func GetBlockSize(path string) (uint64, error) {
	pathPtr, err := windows.UTF16PtrFromString(path)
	if !errors.Is(err, syscall.Errno(0)) {
		return 0, err // nolint:wrapcheck
	}

	var sectorSize, clusterSize uint64

	GetDiskFreeSpaceW := syscall.MustLoadDLL("kernel32.dll").MustFindProc("GetDiskFreeSpaceW")

	_, _, err = GetDiskFreeSpaceW.Call(
		uintptr(*pathPtr),
		uintptr(unsafe.Pointer(&sectorSize)),
		uintptr(unsafe.Pointer(&clusterSize)))
	if !errors.Is(err, syscall.Errno(0)) {
		return 0, err // nolint:wrapcheck
	}

	return sectorSize * clusterSize, nil
}
