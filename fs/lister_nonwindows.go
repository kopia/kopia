// +build !windows

package fs

import "syscall"

func (e *filesystemEntry) UserID() uint32 {
	if stat, ok := e.Sys().(*syscall.Stat_t); ok {
		return stat.Uid
	}

	return 0
}

func (e *filesystemEntry) GroupID() uint32 {
	if stat, ok := e.Sys().(*syscall.Stat_t); ok {
		return stat.Gid
	}

	return 0
}
