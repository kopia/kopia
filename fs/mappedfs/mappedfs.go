package mappedfs

import (
	"context"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot"
)

// FilesystemMapper represents a mapping between paths in the filesystem
type FilesystemMapper interface {
	Apply(path string) (string, error)
	Close()
}

// New returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
func New(entry fs.Entry, fsm FilesystemMapper) (fs.Entry, error) {
	path := entry.LocalFilesystemPath()

	se := &snapshotEntry{
		originalRoot: path,
		mappedRoot:   path,
		fsm:          fsm,
	}

	return se.wrapChild(entry, true)
}

type snapshotEntry struct {
	fs.Entry
	originalRoot string
	mappedRoot   string
	fsm          FilesystemMapper
	isRoot       bool
}

func (s *snapshotEntry) LocalFilesystemPath() string {
	return s.toOriginal(s.Entry.LocalFilesystemPath())
}

func (s *snapshotEntry) isMapped() bool {
	return s.originalRoot != s.mappedRoot
}

func (s *snapshotEntry) toOriginal(mappedPath string) string {
	if !s.isMapped() {
		return mappedPath
	}

	relative, err := filepath.Rel(s.mappedRoot, mappedPath)
	if err != nil {
		// Should never happen
		panic(err)
	}

	return filepath.Join(s.originalRoot, relative)
}

func (s *snapshotEntry) wrapChild(e fs.Entry, isRoot bool) (fs.Entry, error) {
	switch et := e.(type) {
	case fs.Directory:
		originalRoot := s.originalRoot
		mappedRoot := s.mappedRoot

		mappedPath := e.LocalFilesystemPath()
		originalPath := s.toOriginal(mappedPath)

		correctMappedPath, err := s.applyMapping(originalPath)
		if err != nil {
			return nil, err
		}

		if correctMappedPath != mappedPath {
			// Changed snapshot location
			en, err := localfs.NewEntry(correctMappedPath)
			if err != nil {
				//nolint:wrapcheck
				return nil, err
			}

			dir, ok := en.(fs.Directory)
			if !ok {
				return nil, errors.Errorf("should be a directory: %v", correctMappedPath)
			}

			originalRoot = originalPath
			mappedRoot = correctMappedPath
			e = en
			et = dir
		}

		return &snapshotDirectory{
			snapshotEntry: snapshotEntry{
				Entry:        e,
				originalRoot: originalRoot,
				mappedRoot:   mappedRoot,
				fsm:          s.fsm,
				isRoot:       isRoot,
			},
			directory: et,
		}, nil

	case fs.File:
		return &snapshotFile{
			snapshotEntry: snapshotEntry{
				Entry:        e,
				originalRoot: s.originalRoot,
				mappedRoot:   s.mappedRoot,
				fsm:          s.fsm,
				isRoot:       isRoot,
			},
			file: et,
		}, nil

	case fs.Symlink:
		return &snapshotSymlink{
			snapshotEntry: snapshotEntry{
				Entry:        e,
				originalRoot: s.originalRoot,
				mappedRoot:   s.mappedRoot,
				fsm:          s.fsm,
				isRoot:       isRoot,
			},
			symlink: et,
		}, nil

	case fs.ErrorEntry:
		return e, nil

	default:
		return nil, errors.Errorf("Unknown entry: %t %v", e, e)
	}
}

func (s *snapshotEntry) applyMapping(originalPath string) (string, error) {
	snapshotPath, err := s.fsm.Apply(originalPath)
	if err != nil {
		//nolint:wrapcheck
		return "", err
	}

	if snapshotPath[len(snapshotPath)-1] == filepath.Separator {
		snapshotPath = snapshotPath[:len(snapshotPath)-1]
	}

	return snapshotPath, nil
}

func (s *snapshotEntry) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	if defp, ok := s.Entry.(snapshot.HasDirEntryOrNil); ok {
		//nolint:wrapcheck
		return defp.DirEntryOrNil(ctx)
	}

	return nil, nil
}

func (s *snapshotEntry) Close() {
	if s.isRoot {
		s.fsm.Close()
	} else {
		s.Entry.Close()
	}
}

type snapshotDirectory struct {
	snapshotEntry
	directory fs.Directory
}

func (s *snapshotDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	e, err := s.directory.Child(ctx, name)
	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	return s.wrapChild(e, false)
}

func (s *snapshotDirectory) IterateEntries(ctx context.Context, cb func(context.Context, fs.Entry) error) error {
	//nolint:wrapcheck
	return s.directory.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		ee, err := s.wrapChild(e, false)
		if err != nil {
			return err
		}

		return cb(ctx, ee)
	})
}

func (s *snapshotDirectory) SupportsMultipleIterations() bool {
	return s.directory.SupportsMultipleIterations()
}

type snapshotFile struct {
	snapshotEntry
	file fs.File
}

func (s *snapshotFile) Open(ctx context.Context) (fs.Reader, error) {
	r, err := s.file.Open(ctx)
	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	return &snapshotReader{
		reader: r,
		entry:  s,
	}, nil
}

type snapshotSymlink struct {
	snapshotEntry
	symlink fs.Symlink
}

func (s *snapshotSymlink) Readlink(ctx context.Context) (string, error) {
	//nolint:wrapcheck
	return s.symlink.Readlink(ctx)
}

type snapshotReader struct {
	reader fs.Reader
	entry  *snapshotFile
}

func (s *snapshotReader) Read(p []byte) (n int, err error) {
	//nolint:wrapcheck
	return s.reader.Read(p)
}

func (s *snapshotReader) Seek(offset int64, whence int) (int64, error) {
	//nolint:wrapcheck
	return s.reader.Seek(offset, whence)
}

func (s *snapshotReader) Entry() (fs.Entry, error) {
	return s.entry, nil
}

func (s *snapshotReader) Close() error {
	//nolint:wrapcheck
	return s.reader.Close()
}

var (
	_ snapshot.HasDirEntryOrNil = &snapshotDirectory{}
	_ snapshot.HasDirEntryOrNil = &snapshotFile{}
	_ fs.Directory              = &snapshotDirectory{}
	_ fs.File                   = &snapshotFile{}
	_ fs.Symlink                = &snapshotSymlink{}
)
