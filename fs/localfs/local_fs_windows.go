package localfs

import (
	"os"
	"runtime"
	"strings"
	"syscall"

	"github.com/kopia/kopia/fs"
)

var isWindows = runtime.GOOS == "windows"

func platformSpecificOwnerInfo(_ os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func platformSpecificDeviceInfo(_ os.FileInfo) fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func platformSpecificBirthTime(fi os.FileInfo, _ string) int64 {
	if stat, ok := fi.Sys().(*syscall.Win32FileAttributeData); ok {
		// Windows stores creation time in the Filetime structure
		// Convert from Windows FILETIME (100-nanosecond intervals since Jan 1, 1601)
		// to Unix nanoseconds (nanoseconds since Jan 1, 1970)
		return stat.CreationTime.Nanoseconds()
	}
	return 0
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
