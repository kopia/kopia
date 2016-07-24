package fs

import "log"

type loggingDirectory struct {
	Directory
}

func (ld *loggingDirectory) Readdir() (Entries, error) {
	log.Printf("Entries(%v)", EntryPath(ld))
	entries, err := ld.Directory.Readdir()
	loggingEntries := make(Entries, len(entries))
	for i, entry := range entries {
		loggingEntries[i] = newLoggingWrapper(entry)
	}
	return loggingEntries, err
}

type loggingFile struct {
	File
}

type loggingSymlink struct {
	Symlink
}

// newLoggingWrapper returns an Entry that wraps another Entry and logs all method calls.
func newLoggingWrapper(e Entry) Entry {
	switch e := e.(type) {
	case Directory:
		return Directory(&loggingDirectory{e})

	case File:
		return File(&loggingFile{e})

	case Symlink:
		return Symlink(&loggingSymlink{e})

	default:
		return e
	}
}

var _ Directory = &loggingDirectory{}
var _ File = &loggingFile{}
var _ Symlink = &loggingSymlink{}
