package localfs

import (
	"os"
	"runtime"
	"strings"

	"github.com/kopia/kopia/fs"
)

var isWindows = runtime.GOOS == "windows"

func platformSpecificOwnerInfo(_ os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func platformSpecificDeviceInfo(_ os.FileInfo) fs.DeviceInfo {
	return fs.DeviceInfo{}
}

// Direct Windows volume paths (e.g. Shadow Copy) require a trailing separator.
func trailingSeparator(fsd *filesystemDirectory) string {
	// is fsd a Windows VSS Volume and has no trailing separator?
	if isWindows &&
		fsd.prefix == `\\?\GLOBALROOT\Device\` &&
		strings.HasPrefix(fsd.Name(), "HarddiskVolumeShadowCopy") &&
		!strings.HasSuffix(fsd.Name(), separatorStr) {

		return separatorStr
	}

	return ""
}

// TODO: if we want to implement it for windows, it will be tricky. On Windows, the value returned by os.FileInfo.Sys() is typically a pointer to a syscall.Win32FileAttributeData structure, which does not include the file's MFT or file ID. In other words, unlike on Unix where you can extract the inode number directly from the stat structure, Windows does not expose the file ID through os.FileInfo.
func platformSpecificNewEntry(basename string, fi os.FileInfo, prefix string) filesystemEntry {
	return filesystemEntry{
		TrimShallowSuffix(basename),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		platformSpecificHardLinkInfo(fi),
		prefix,
	}
}

//nolint:revive
func platformSpecificHardLinkInfo(fi os.FileInfo) fs.HardLinkInfo {
	return fs.HardLinkInfo{}
}
