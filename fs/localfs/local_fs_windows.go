package localfs

import (
	"os"

	"github.com/kopia/kopia/fs"
)

func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	return fs.OwnerInfo{}
}
