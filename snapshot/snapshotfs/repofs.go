// Package snapshotfs implements virtual filesystem on top of snapshots in repo.Repository.
package snapshotfs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/pkg/errors"
)

type repositoryEntry struct {
	metadata *snapshot.DirEntry
	repo     *repo.Repository
}

func (e *repositoryEntry) IsDir() bool {
	return e.Mode().IsDir()
}

func (e *repositoryEntry) Mode() os.FileMode {
	switch e.metadata.Type {
	case snapshot.EntryTypeDirectory:
		return os.ModeDir | os.FileMode(e.metadata.Permissions)
	case snapshot.EntryTypeSymlink:
		return os.ModeSymlink | os.FileMode(e.metadata.Permissions)
	default:
		return os.FileMode(e.metadata.Permissions)
	}
}

func (e *repositoryEntry) Name() string {
	return e.metadata.Name
}

func (e *repositoryEntry) Size() int64 {
	return e.metadata.FileSize
}

func (e *repositoryEntry) ModTime() time.Time {
	return e.metadata.ModTime
}

func (e *repositoryEntry) ObjectID() object.ID {
	return e.metadata.ObjectID
}

func (e *repositoryEntry) Sys() interface{} {
	return nil
}

func (e *repositoryEntry) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{
		UserID:  e.metadata.UserID,
		GroupID: e.metadata.GroupID,
	}
}

type repositoryDirectory struct {
	repositoryEntry
	summary *fs.DirectorySummary
}

type repositoryFile struct {
	repositoryEntry
}

type repositorySymlink struct {
	repositoryEntry
}

func (rd *repositoryDirectory) Summary() *fs.DirectorySummary {
	return rd.summary
}

func (rd *repositoryDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	r, err := rd.repo.Objects.Open(ctx, rd.metadata.ObjectID)
	if err != nil {
		return nil, err
	}
	defer r.Close() //nolint:errcheck

	metadata, _, err := readDirEntries(r)
	if err != nil {
		return nil, err
	}

	entries := make(fs.Entries, len(metadata))
	for i, m := range metadata {
		entries[i] = newRepoEntry(rd.repo, m)
	}

	entries.Sort()

	return entries, nil
}

func (rf *repositoryFile) Open(ctx context.Context) (fs.Reader, error) {
	r, err := rf.repo.Objects.Open(ctx, rf.metadata.ObjectID)
	if err != nil {
		return nil, err
	}

	return withFileInfo(r, rf), nil
}

func (rsl *repositorySymlink) Readlink(ctx context.Context) (string, error) {
	r, err := rsl.repo.Objects.Open(ctx, rsl.metadata.ObjectID)
	if err != nil {
		return "", err
	}

	defer r.Close() //nolint:errcheck
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func newRepoEntry(r *repo.Repository, md *snapshot.DirEntry) fs.Entry {
	re := repositoryEntry{
		metadata: md,
		repo:     r,
	}

	switch md.Type {
	case snapshot.EntryTypeDirectory:
		if md.DirSummary != nil {
			md.FileSize = md.DirSummary.TotalFileSize
			md.ModTime = md.DirSummary.MaxModTime
		}

		return fs.Directory(&repositoryDirectory{re, md.DirSummary})

	case snapshot.EntryTypeSymlink:
		return fs.Symlink(&repositorySymlink{re})

	case snapshot.EntryTypeFile:
		return fs.File(&repositoryFile{re})

	default:
		panic(fmt.Sprintf("not supported entry metadata type: %v", md.Type))
	}
}

type readCloserWithFileInfo struct {
	object.Reader
	e fs.Entry
}

func (r *readCloserWithFileInfo) Entry() (fs.Entry, error) {
	return r.e, nil
}

func withFileInfo(r object.Reader, e fs.Entry) fs.Reader {
	return &readCloserWithFileInfo{r, e}
}

// DirectoryEntry returns fs.Directory based on repository object with the specified ID.
// The existence or validity of the directory object is not validated until its contents are read.
func DirectoryEntry(rep *repo.Repository, objectID object.ID, dirSummary *fs.DirectorySummary) fs.Directory {
	d := newRepoEntry(rep, &snapshot.DirEntry{
		Name:        "/",
		Permissions: 0555,
		Type:        snapshot.EntryTypeDirectory,
		ObjectID:    objectID,
		DirSummary:  dirSummary,
	})

	return d.(fs.Directory)
}

// SnapshotRoot returns fs.Entry representing the root of a snapshot.
func SnapshotRoot(rep *repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	oid := man.RootObjectID()
	if oid == "" {
		return nil, errors.New("manifest root object ID")
	}

	return newRepoEntry(rep, man.RootEntry), nil
}

var _ fs.Directory = &repositoryDirectory{}
var _ fs.File = &repositoryFile{}
var _ fs.Symlink = &repositorySymlink{}
