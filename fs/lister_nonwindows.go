// +build !windows

package fs

import (
	"os"
	"syscall"
)

func (e *Entry) populatePlatformSpecificEntryDetails(fi os.FileInfo) error {
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.OwnerID = stat.Uid
		e.GroupID = stat.Gid
	}

	return nil
}
