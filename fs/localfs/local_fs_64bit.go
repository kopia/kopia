// +build !windows,!openbsd
// +build !darwin
// +build amd64 arm64 arm 386

package localfs

func platformSpecificWidenDev(dev uint64) uint64 {
	return dev
}
