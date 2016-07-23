package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/kopia/kopia/repo"
)

type repoEntry struct {
	entry
	repo repo.Repository
}

type repoDirectory repoEntry
type repoFile repoEntry
type repoSymlink repoEntry

func (rd *repoDirectory) Readdir() (Entries, error) {
	r, err := rd.repo.Open(rd.entry.Metadata().ObjectID)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	metadata, err := ReadDirectoryMetadataEntries(r, "")
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, len(metadata))
	for i, m := range metadata {
		entries[i] = newRepoEntry(rd.repo, m, rd)
	}

	return entries, nil
}

func (rf *repoFile) Open() (EntryMetadataReadCloser, error) {
	r, err := rf.repo.Open(rf.entry.Metadata().ObjectID)
	if err != nil {
		return nil, err
	}

	return withMetadata(r, rf.entry.Metadata()), nil
}

func (rsl *repoSymlink) Readlink() (string, error) {
	panic("not implemented yet")
}

func newRepoEntry(r repo.Repository, md *EntryMetadata, parent Directory) Entry {
	switch md.FileMode & os.ModeType {
	case os.ModeDir:
		return Directory(&repoDirectory{
			entry: newEntry(md, parent),
			repo:  r,
		})

	case os.ModeSymlink:
		return Symlink(&repoSymlink{
			entry: newEntry(md, parent),
			repo:  r,
		})

	case 0:
		return File(&repoFile{
			entry: newEntry(md, parent),
			repo:  r,
		})

	default:
		panic(fmt.Sprintf("not supported entry metadata type: %v", md.FileMode))
	}
}

func NewRootDirectoryFromRepository(r repo.Repository, oid repo.ObjectID) Directory {
	d := newRepoEntry(r, &EntryMetadata{
		Name:     "/",
		ObjectID: oid,
		FileMode: 0555 | os.ModeDir,
	}, nil)

	return d.(Directory)
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

var _ Directory = &repoDirectory{}
var _ File = &repoFile{}
var _ Symlink = &repoSymlink{}
