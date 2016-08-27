package upload

import "github.com/kopia/kopia/fs"

const (
	entryTypeBundle fs.EntryType = "b" // bundle
)

type bundle struct {
	metadata *fs.EntryMetadata
	files    []fs.File
}

func (b *bundle) Parent() fs.Directory {
	return nil
}

func (b *bundle) Metadata() *fs.EntryMetadata {
	return b.metadata
}

func (b *bundle) append(e fs.File) {
	b.files = append(b.files, e)
	b.metadata.FileSize += e.Metadata().FileSize
	emt := e.Metadata().ModTime
	if b.metadata.ModTime.IsZero() || b.metadata.ModTime.Before(emt) {
		b.metadata.ModTime = emt
	}
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

func (f *bundledFile) Open() (fs.EntryMetadataReadCloser, error) {
	panic("Open() is not meant to be called")
}
