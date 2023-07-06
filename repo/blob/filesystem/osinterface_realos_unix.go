//go:build linux || freebsd || darwin
// +build linux freebsd darwin

package filesystem

import (
	"syscall"

	"github.com/pkg/errors"
)

func (realOS) IsStale(err error) bool {
	var errno syscall.Errno

	return errors.As(err, &errno) && errno == syscall.ESTALE
}
