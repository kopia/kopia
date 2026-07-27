//go:build linux

// Package iomem provides OS-specific helpers to advise the kernel
// about page cache usage for accessed files.
package iomem

import (
	"os"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// HintStreaming advises the kernel that this file will be read sequentially,
// once. Call it immediately after opening a file for backup so the hint
// lands before any reads populate the page cache.
func HintStreaming(f *os.File) error {
	err := callWithFd(f, func(fd int) error {
		return unix.Fadvise(fd, 0, 0, unix.FADV_SEQUENTIAL)
	})

	return errors.Wrap(err, "FADV_SEQUENTIAL hint for streaming file reads failed")
}

// HintNotNeeded advises the kernel that cached pages for this file are no
// longer needed and can be reclaimed. Call it after reading is done.
func HintNotNeeded(f *os.File) error {
	err := callWithFd(f, func(fd int) error {
		return unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED)
	})

	return errors.Wrap(err, "FADV_DONTNEED hint for releasing file I/O memory failed")
}

// callWithFd runs op against f's underlying file descriptor.
//
// It uses SyscallConn().Control() rather than f.Fd() for two reasons:
//
//   - Async/blocking: on Linux, regular files opened via os.Open are
//     put into non-blocking mode and registered with Go's runtime
//     poller. f.Fd() would flip the descriptor back to blocking and
//     detach it from the poller, so subsequent blocking syscalls on
//     the file would tie up an OS thread per concurrent caller.
//   - Lifecycle: Control() also pins the fd via the runtime's
//     incref/decref guard for the duration of op, so a concurrent
//     f.Close() cannot invalidate the descriptor (or trigger an
//     fd-reuse race) before the syscall returns.
func callWithFd(f *os.File, op func(fd int) error) error {
	if f == nil {
		return errors.New("nil file")
	}

	conn, err := f.SyscallConn()
	if err != nil {
		return errors.Wrap(err, "SyscallConn")
	}

	var opErr error

	if err := conn.Control(func(fd uintptr) {
		opErr = op(int(fd))
	}); err != nil {
		return errors.Wrap(err, "Control")
	}

	return opErr
}
