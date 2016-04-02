// +build !windows

package fs

import (
	"fmt"
	"os"
	"syscall"
)

func populatePlatformSpecificEntryDetails(e *Entry, fileInfo os.FileInfo) error {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		e.UserID = stat.Uid
		e.GroupID = stat.Gid
		return nil
	}

	return fmt.Errorf("unable to retrieve platform-specific file information")
}
