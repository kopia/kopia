package localfs

import (
	"os"
	"runtime"
	"strings"

	"github.com/kopia/kopia/fs"
)

const isWindows = runtime.GOOS == "windows"

func platformSpecificOwnerInfo(_ os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func platformSpecificDeviceInfo(_ os.FileInfo) fs.DeviceInfo {
	return fs.DeviceInfo{}
}

// On Windows, os.FileInfo.Sys() is a *syscall.Win32FileAttributeData, which does not
// carry the NTFS file ID or link count, so hardlink info is not available without
// an additional GetFileInformationByHandle call.
func platformSpecificHardLinkInfo(_ os.FileInfo) fs.HardLinkInfo {
	return fs.HardLinkInfo{}
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

