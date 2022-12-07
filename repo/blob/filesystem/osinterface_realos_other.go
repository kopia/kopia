//go:build !linux && !freebsd && !darwin
// +build !linux,!freebsd,!darwin

package filesystem

func (realOS) IsESTALE(err error) bool {
	return false
}
