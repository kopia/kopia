package localfs

import (
	"os"

	"github.com/kopia/kopia/fs"
)

//nolint:revive
func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

//nolint:revive
func platformSpecificDeviceInfo(fi os.FileInfo) fs.DeviceInfo {
	return fs.DeviceInfo{}
}

//nolint:revive
func platformSpecificAttributesInfo(f *os.File, prefix string) fs.AttributesInfo {
	return nil
}
