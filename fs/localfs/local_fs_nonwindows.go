// +build !windows

package localfs

import (
	"os"
	"syscall"

	"github.com/kopia/kopia/fs"
)

func populatePlatformSpecificEntryDetails(e *fs.EntryMetadata, fi os.FileInfo) error {
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.UserID = stat.Uid
		e.GroupID = stat.Gid
	}

	return nil
}
