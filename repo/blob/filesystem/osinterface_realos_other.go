//go:build !linux && !freebsd && !darwin
// +build !linux,!freebsd,!darwin

package filesystem

//nolint:revive
func (realOS) IsESTALE(err error) bool {
	return false
}
