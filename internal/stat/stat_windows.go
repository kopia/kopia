//go:build windows
// +build windows

// Package stat provides a cross-platform abstraction for
// common stat commands.
package stat

import (
	"math"
	"runtime"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

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
	kernel32 := windows.NewLazyDLL("kernel32.dll")
	getDiskFreeSpace := kernel32.NewProc("GetDiskFreeSpaceW")

	var sectorsPerCluster, bytesPerSector, freeClusters, totalClusters uint32

	ret, _, err := getDiskFreeSpace.Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&freeClusters)),
		uintptr(unsafe.Pointer(&totalClusters)),
	)
	if ret == 0 {
		return math.MaxUint64, errors.Wrapf(err, "error while getting block size for %v", runtime.GOOS)
	}

	// Calculate the block size as sectors per cluster * bytes per sector
	return uint64(sectorsPerCluster) * uint64(bytesPerSector), nil
}
