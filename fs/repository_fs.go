package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/kopia/kopia/repo"
)

type repositoryEntry struct {
	entry
	repo repo.Repository
}

type repositoryDirectory repositoryEntry
type repositoryFile repositoryEntry
type repositorySymlink repositoryEntry

func (rd *repositoryDirectory) Readdir() (Entries, error) {
	r, err := rd.repo.Open(rd.entry.Metadata().ObjectID)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	metadata, err := readDirectoryMetadataEntries(r)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, len(metadata))
	for i, m := range metadata {
		entries[i] = newRepoEntry(rd.repo, m, rd)
	}

	return entries, nil
}

func (rf *repositoryFile) Open() (EntryMetadataReadCloser, error) {
	r, err := rf.repo.Open(rf.entry.Metadata().ObjectID)
	if err != nil {
		return nil, err
	}

	return withMetadata(r, rf.entry.Metadata()), nil
}

func (rsl *repositorySymlink) Readlink() (string, error) {
	panic("not implemented yet")
}

func newRepoEntry(r repo.Repository, md *EntryMetadata, parent Directory) Entry {
	switch md.Mode & os.ModeType {
	case os.ModeDir:
		return Directory(&repositoryDirectory{
			entry: newEntry(md, parent),
			repo:  r,
		})

	case os.ModeSymlink:
		return Symlink(&repositorySymlink{
			entry: newEntry(md, parent),
			repo:  r,
		})

	case 0:
		return File(&repositoryFile{
			entry: newEntry(md, parent),
			repo:  r,
		})

	default:
		panic(fmt.Sprintf("not supported entry metadata type: %v", md.Mode))
	}
}

type entryMetadataReadCloser struct {
	io.ReadCloser
	metadata *EntryMetadata
}

func (emrc *entryMetadataReadCloser) EntryMetadata() (*EntryMetadata, error) {
	return emrc.metadata, nil
}

func withMetadata(rc io.ReadCloser, md *EntryMetadata) EntryMetadataReadCloser {
	return &entryMetadataReadCloser{
		rc,
		md,
	}
}

// NewRepositoryDirectory returns Directory based on repository object with the specified ID.
func NewRepositoryDirectory(r repo.Repository, objectID repo.ObjectID) Directory {
	d := newRepoEntry(r, &EntryMetadata{
		Name:     "/",
		ObjectID: objectID,
		Mode:     0555 | os.ModeDir,
	}, nil)

	return d.(Directory)
}

var _ Directory = &repositoryDirectory{}
var _ File = &repositoryFile{}
var _ Symlink = &repositorySymlink{}
