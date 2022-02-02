// Package snapshotfs implements virtual filesystem on top of snapshots in repo.Repository.
package snapshotfs

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// Well-known object ID prefixes.
const (
	objectIDPrefixDirectory = "k"
)

type repositoryEntry struct {
	metadata *snapshot.DirEntry
	repo     repo.Repository
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
	case snapshot.EntryTypeFile:
		return os.FileMode(e.metadata.Permissions)
	case snapshot.EntryTypeUnknown:
		return 0
	default:
		return 0
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

func (e *repositoryEntry) Device() fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func (e *repositoryEntry) DirEntry() *snapshot.DirEntry {
	return e.metadata
}

func (e *repositoryEntry) LocalFilesystemPath() string {
	return ""
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

type repositoryEntryError struct {
	repositoryEntry
	err error
}

func (rd *repositoryDirectory) Summary(ctx context.Context) (*fs.DirectorySummary, error) {
	if rd.summary != nil {
		return rd.summary, nil
	}

	r, err := rd.repo.OpenObject(ctx, rd.metadata.ObjectID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open object: %v", rd.metadata.ObjectID)
	}
	defer r.Close() //nolint:errcheck

	_, summ, err := readDirEntries(r)
	if err != nil {
		return nil, err
	}

	return summ, nil
}

func (rd *repositoryDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	// nolint:wrapcheck
	return fs.ReadDirAndFindChild(ctx, rd, name)
}

func (rd *repositoryDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	r, err := rd.repo.OpenObject(ctx, rd.metadata.ObjectID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open object: %v", rd.metadata.ObjectID)
	}
	defer r.Close() //nolint:errcheck

	metadata, _, err := readDirEntries(r)
	if err != nil {
		return nil, err
	}

	entries := make(fs.Entries, len(metadata))
	for i, m := range metadata {
		entries[i] = EntryFromDirEntry(rd.repo, m)
	}

	entries.Sort()

	return entries, nil
}

func (rf *repositoryFile) Open(ctx context.Context) (fs.Reader, error) {
	r, err := rf.repo.OpenObject(ctx, rf.metadata.ObjectID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open object: %v", rf.metadata.ObjectID)
	}

	return withFileInfo(r, rf), nil
}

func (rsl *repositorySymlink) Readlink(ctx context.Context) (string, error) {
	r, err := rsl.repo.OpenObject(ctx, rsl.metadata.ObjectID)
	if err != nil {
		return "", errors.Wrapf(err, "unable to open object: %v", rsl.metadata.ObjectID)
	}

	defer r.Close() //nolint:errcheck

	b, err := io.ReadAll(r)
	if err != nil {
		return "", errors.Wrapf(err, "unable to read object: %v", rsl.metadata.ObjectID)
	}

	return string(b), nil
}

func (ee *repositoryEntryError) ErrorInfo() error {
	return ee.err
}

// EntryFromDirEntry returns a filesystem entry based on the directory entry.
func EntryFromDirEntry(r repo.Repository, md *snapshot.DirEntry) fs.Entry {
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
		return fs.ErrorEntry(&repositoryEntryError{re, fs.ErrUnknown})
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
func DirectoryEntry(rep repo.Repository, objectID object.ID, dirSummary *fs.DirectorySummary) fs.Directory {
	d := EntryFromDirEntry(rep, &snapshot.DirEntry{
		Name:        "/",
		Permissions: 0o555, //nolint:gomnd
		Type:        snapshot.EntryTypeDirectory,
		ObjectID:    objectID,
		DirSummary:  dirSummary,
	})

	return d.(fs.Directory) // nolint:forcetypeassert
}

// SnapshotRoot returns fs.Entry representing the root of a snapshot.
func SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	oid := man.RootObjectID()
	if oid == "" {
		return nil, errors.New("manifest root object ID")
	}

	return EntryFromDirEntry(rep, man.RootEntry), nil
}

// AutoDetectEntryFromObjectID returns fs.Entry (either file or directory) for the provided object ID.
// It uses heuristics to determine whether object ID is possibly a directory and treats it as such.
func AutoDetectEntryFromObjectID(ctx context.Context, rep repo.Repository, oid object.ID, maybeName string) fs.Entry {
	if IsDirectoryID(oid) {
		dirEntry := DirectoryEntry(rep, oid, nil)
		if _, err := dirEntry.Readdir(ctx); err == nil {
			repoFSLog(ctx).Debugf("%v auto-detected as directory", oid)
			return dirEntry
		}
	}

	if maybeName == "" {
		maybeName = "file"
	}

	var fileSize int64

	r, err := rep.OpenObject(ctx, oid)
	if err == nil {
		fileSize = r.Length()
		r.Close() //nolint:errcheck
	}

	repoFSLog(ctx).Debugf("%v auto-detected as a file with name %v and size %v", oid, maybeName, fileSize)

	f := EntryFromDirEntry(rep, &snapshot.DirEntry{
		Name:        maybeName,
		Permissions: 0o644, //nolint:gomnd
		Type:        snapshot.EntryTypeFile,
		ObjectID:    oid,
		FileSize:    fileSize,
	})

	return f
}

// IsDirectoryID determines whether given object ID represents a directory.
func IsDirectoryID(oid object.ID) bool {
	if ndx, ok := oid.IndexObjectID(); ok {
		return IsDirectoryID(ndx)
	}

	if cid, _, ok := oid.ContentID(); ok {
		return cid.Prefix() == objectIDPrefixDirectory
	}

	return false
}

var (
	_ fs.Directory = (*repositoryDirectory)(nil)
	_ fs.File      = (*repositoryFile)(nil)
	_ fs.Symlink   = (*repositorySymlink)(nil)
)

var (
	_ snapshot.HasDirEntry = (*repositoryDirectory)(nil)
	_ snapshot.HasDirEntry = (*repositoryFile)(nil)
	_ snapshot.HasDirEntry = (*repositorySymlink)(nil)
)
