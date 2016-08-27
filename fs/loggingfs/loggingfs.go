// Package loggingfs implements a wrapper that logs all filesystem actions.
package loggingfs

import (
	"log"

	"github.com/kopia/kopia/fs"
)

type loggingDirectory struct {
	fs.Directory
}

func (ld *loggingDirectory) Readdir() (fs.Entries, error) {
	log.Printf("Readdir(%v)", fs.EntryPath(ld))
	entries, err := ld.Directory.Readdir()
	loggingEntries := make(fs.Entries, len(entries))
	for i, entry := range entries {
		loggingEntries[i] = Wrap(entry)
	}
	return loggingEntries, err
}

type loggingFile struct {
	fs.File
}

type loggingSymlink struct {
	fs.Symlink
}

// Wrap returns an Entry that wraps another Entry and logs all method calls.
func Wrap(e fs.Entry) fs.Entry {
	switch e := e.(type) {
	case fs.Directory:
		return fs.Directory(&loggingDirectory{e})

	case fs.File:
		return fs.File(&loggingFile{e})

	case fs.Symlink:
		return fs.Symlink(&loggingSymlink{e})

	default:
		return e
	}
}

var _ fs.Directory = &loggingDirectory{}
var _ fs.File = &loggingFile{}
var _ fs.Symlink = &loggingSymlink{}
