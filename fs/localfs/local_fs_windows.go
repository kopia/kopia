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
	// is fsd a Windows VSS Volume and has no trailing
	if isWindows &&
		fsd.prefix == `\\?\GLOBALROOT\Device\` &&
		strings.HasPrefix(fsd.Name(), "HarddiskVolumeShadowCopy") &&
		!strings.HasSuffix(fsd.Name(), separatorStr) {

		return separatorStr
	}

	return ""
}
