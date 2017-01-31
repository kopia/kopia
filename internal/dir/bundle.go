package dir

import "github.com/kopia/kopia/fs"

// EntryTypeBundle is the identifier of filesystem bundle.
const EntryTypeBundle fs.EntryType = "b"

// Bundle represents a collection of files stored together to minimize the number of storage objects.
type Bundle struct {
	metadata *fs.EntryMetadata
	Files    []fs.File
}

// Parent returns the parent directory of the bundle.
func (b *Bundle) Parent() fs.Directory {
	return nil
}

// Metadata returns the bundle metadata.
func (b *Bundle) Metadata() *fs.EntryMetadata {
	return b.metadata
}

// Append adds a given file to the bundle.
func (b *Bundle) Append(e fs.File) {
	b.Files = append(b.Files, e)
	b.metadata.FileSize += e.Metadata().FileSize
	emt := e.Metadata().ModTime
	if b.metadata.ModTime.IsZero() || b.metadata.ModTime.Before(emt) {
		b.metadata.ModTime = emt
	}
}

// NewBundle creates a new bundle with given metadata.
func NewBundle(metadata *fs.EntryMetadata) *Bundle {
	return &Bundle{metadata, nil}
}

type bundledFile struct {
	metadata *fs.EntryMetadata
}

func (f *bundledFile) Parent() fs.Directory {
	return nil
}

func (f *bundledFile) Metadata() *fs.EntryMetadata {
	return f.metadata
}

func (f *bundledFile) Open() (fs.Reader, error) {
	panic("Open() is not meant to be called")
}

// NewBundledFile returns new bundled file.
func NewBundledFile(metadata *fs.EntryMetadata) fs.File {
	return &bundledFile{metadata}
}
