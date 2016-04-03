package fs

// Directory represents contents of a directory.

type EntryOrError struct {
	Entry *Entry
	Error error
}

type Directory chan EntryOrError

var emptyDirectory Directory

func init() {
	emptyDirectory = make(Directory)
	close(emptyDirectory)
}
