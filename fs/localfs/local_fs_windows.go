package localfs

import (
	"os"

	"github.com/kopia/kopia/fs"
)

func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func platformSpecificDeviceInfo(fi os.FileInfo) fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func platformSpecificNewFileEntry(fi os.FileInfo, parentDir string) fs.Entry {
	return &filesystemFile{newEntry(fi, parentDir)}
}
