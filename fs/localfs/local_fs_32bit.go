//go:build !windows && ((!amd64 && !arm64 && !arm && !ppc64 && !ppc64le && !s390x && !386 && !riscv64) || darwin || openbsd)
// +build !windows
// +build !amd64,!arm64,!arm,!ppc64,!ppc64le,!s390x,!386,!riscv64 darwin openbsd

package localfs

func platformSpecificWidenDev(dev int32) uint64 {
	return uint64(dev) //nolint:gosec
}
