// +build !windows
// +build !amd64,!arm64,!arm,!386 darwin openbsd

package localfs

func platformSpecificWidenDev(dev int32) uint64 {
	return uint64(dev)
}
