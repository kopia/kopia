// Package repofs implements virtual filesystem on top of Repository.
package repofs

import (
	"fmt"
	"io"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/dirstream"
	"github.com/kopia/kopia/repo"
)

type repositoryEntry struct {
	parent   fs.Directory
	metadata *fs.EntryMetadata
	repo     *repo.Repository
}

func (e *repositoryEntry) Parent() fs.Directory {
	return e.parent
}

func (e *repositoryEntry) Metadata() *fs.EntryMetadata {
	return e.metadata
}

type repositoryDirectory struct {
	repositoryEntry
}

type repositoryFile struct {
	repositoryEntry
}

type repositorySymlink struct {
	repositoryEntry
}

func (rd *repositoryDirectory) Readdir() (fs.Entries, error) {
	r, err := rd.repo.Open(rd.metadata.ObjectID)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	metadata, err := dirstream.ReadEntries(r)
	if err != nil {
		return nil, err
	}

	entries := make(fs.Entries, len(metadata))
	for i, m := range metadata {
		entries[i] = newRepoEntry(rd.repo, m, rd)
	}

	return entries, nil
}

func (rf *repositoryFile) Open() (fs.Reader, error) {
	r, err := rf.repo.Open(rf.Metadata().ObjectID)
	if err != nil {
		return nil, err
	}

	return withMetadata(r, rf.metadata), nil
}

func (rsl *repositorySymlink) Readlink() (string, error) {
	panic("not implemented yet")
}

func newRepoEntry(r *repo.Repository, md *fs.EntryMetadata, parent fs.Directory) fs.Entry {
	re := repositoryEntry{
		metadata: md,
		parent:   parent,
		repo:     r,
	}
	switch md.Type {
	case fs.EntryTypeDirectory:
		return fs.Directory(&repositoryDirectory{re})

	case fs.EntryTypeSymlink:
		return fs.Symlink(&repositorySymlink{re})

	case fs.EntryTypeFile:
		return fs.File(&repositoryFile{re})

	default:
		panic(fmt.Sprintf("not supported entry metadata type: %v", md.Type))
	}
}

type entryMetadataReadCloser struct {
	io.ReadCloser
	metadata *fs.EntryMetadata
}

func (emrc *entryMetadataReadCloser) EntryMetadata() (*fs.EntryMetadata, error) {
	return emrc.metadata, nil
}

func withMetadata(rc io.ReadCloser, md *fs.EntryMetadata) fs.Reader {
	return &entryMetadataReadCloser{
		rc,
		md,
	}
}

// Directory returns fs.Directory based on repository object with the specified ID.
// The existence or validity of the directory object is not validated until its contents are read.
func Directory(r *repo.Repository, objectID repo.ObjectID) fs.Directory {
	d := newRepoEntry(r, &fs.EntryMetadata{
		Name:        "/",
		ObjectID:    objectID,
		Permissions: 0555,
		Type:        fs.EntryTypeDirectory,
	}, nil)

	return d.(fs.Directory)
}

var _ fs.Directory = &repositoryDirectory{}
var _ fs.File = &repositoryFile{}
var _ fs.Symlink = &repositorySymlink{}
