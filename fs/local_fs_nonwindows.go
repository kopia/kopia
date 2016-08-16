// +build !windows

package fs

import (
	"os"
	"syscall"
)

func (e *EntryMetadata) populatePlatformSpecificEntryDetails(fi os.FileInfo) error {
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		e.Uid = stat.Uid
		e.Gid = stat.Gid
	}

	return nil
}
