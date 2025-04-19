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

// TODO: if we want to implement it for windows, it will be tricky. On Windows, the value returned by os.FileInfo.Sys() is typically a pointer to a syscall.Win32FileAttributeData structure, which does not include the file's MFT or file ID. In other words, unlike on Unix where you can extract the inode number directly from the stat structure, Windows does not expose the file ID through os.FileInfo.
func platformSpecificNewEntry(fi os.FileInfo, prefix string) filesystemEntry {
	var uId uint64
	return filesystemEntry{
		TrimShallowSuffix(fi.Name()),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		uId,
		prefix,
	}
}
