//go:build !windows && !openbsd && !darwin && (amd64 || arm64 || arm || ppc64 || ppc64le || s390x || 386 || riscv64)
// +build !windows
// +build !openbsd
// +build !darwin
// +build amd64 arm64 arm ppc64 ppc64le s390x 386 riscv64

package localfs

func platformSpecificWidenDev(dev uint64) uint64 {
	return dev
}
