//go:build !linux

// Package iomem provides OS-specific helpers to advise the kernel
// about page cache usage for accessed files.
package iomem

import (
	"os"

	"github.com/pkg/errors"
)

// HintStreaming is a no-op on non-Linux builds. It errors on a nil file to
// match the Linux implementation's contract (callWithFd in iomem_linux.go).
func HintStreaming(f *os.File) error {
	if f == nil {
		return errors.New("nil file")
	}

	return nil
}

// HintNotNeeded is a no-op on non-Linux builds. It errors on a nil file to
// match the Linux implementation's contract.
func HintNotNeeded(f *os.File) error {
	if f == nil {
		return errors.New("nil file")
	}

	return nil
}
