//go:build !windows && !openbsd && !darwin && (amd64 || arm64 || arm)
// +build !windows
// +build !openbsd
// +build !darwin
// +build amd64 arm64 arm

package localfs

func platformSpecificWidenDev(dev uint64) uint64 {
	return dev
}
