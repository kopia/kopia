package localfs

import (
	"os"
	"runtime"

	"github.com/kopia/kopia/fs"
)

var isWindows = runtime.GOOS == "windows"

//nolint:revive
func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

//nolint:revive
func platformSpecificDeviceInfo(fi os.FileInfo) fs.DeviceInfo {
	return fs.DeviceInfo{}
}
