package fssnapshotfs

import (
	"context"
	"fmt"
	"path/filepath"

	fs_snapshot "github.com/pescuma/go-fs-snapshot/lib"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
)

type snapshotEntry struct {
	snapshoter   fs_snapshot.Snapshoter
	backuper     fs_snapshot.Backuper
	originalRoot string
	snapshotRoot string
	fs.Entry
}

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
func NewEntry(originalPath string) (fs.Entry, error) {
	snapshoter, err := fs_snapshot.CreateSnapshoter()
	if err != nil {
		// If we can't use snapshots, just use the filesystem directly
		fmt.Printf("Error creating filesystem snapshot: %v\n", err)

		//nolint:wrapcheck
		return localfs.NewEntry(originalPath)
	}

	backuper, err := snapshoter.StartBackup(&fs_snapshot.CreateSnapshotOptions{
		InfoCallback: func(level fs_snapshot.MessageLevel, msg string) {
			if level == fs_snapshot.InfoLevel {
				fmt.Println(msg)
			}
		},
	})
	if err != nil {
		// If we can't use snapshots, just use the filesystem directly
		fmt.Printf("Error creating filesystem snapshot: %v\n", err)

		snapshoter.Close()

		//nolint:wrapcheck
		return localfs.NewEntry(originalPath)
	}

	snapshotPath, _ := backuper.TryToCreateTemporarySnapshot(originalPath)

	e, err := localfs.NewEntry(snapshotPath)
	if err != nil {
		backuper.Close()
		snapshoter.Close()

		//nolint:wrapcheck
		return nil, err
	}

	dir, ok := e.(fs.Directory)
	if !ok {
		return nil, errors.Errorf("Filesystem snapshot path should be a directory: %v", snapshotPath)
	}

	return &snapshotRoot{
		snapshotDirectory{
			snapshotEntry: snapshotEntry{
				snapshoter:   snapshoter,
				backuper:     backuper,
				originalRoot: originalPath,
				snapshotRoot: snapshotPath,
				Entry:        e,
			},
			directory: dir,
		},
	}, nil
}

func (s *snapshotEntry) LocalFilesystemPath() string {
	return s.toOriginal(s.Entry.LocalFilesystemPath())
}

func (s *snapshotEntry) toOriginal(snapshotPath string) string {
	relative, err := filepath.Rel(s.snapshotRoot, snapshotPath)
	if err != nil {
		// Should never happen
		panic(err)
	}

	return filepath.Join(s.originalRoot, relative)
}

func (s *snapshotEntry) Close() {
	s.Entry.Close()
}

type snapshotRoot struct {
	snapshotDirectory
}

func (s *snapshotRoot) Close() {
	s.snapshotEntry.Close()
	s.backuper.Close()
	s.snapshoter.Close()
}

type snapshotDirectory struct {
	snapshotEntry
	directory fs.Directory
}

func (s *snapshotDirectory) wrapEntry(e fs.Entry) (fs.Entry, error) {
	switch ee := e.(type) {
	case fs.Directory:
		snapshotPath := e.LocalFilesystemPath()
		originalPath := s.toOriginal(snapshotPath)

		// On case of error just use original dir
		correctSnapshotPath, _ := s.backuper.TryToCreateTemporarySnapshot(originalPath)

		if correctSnapshotPath != snapshotPath {
			// Changed snapshot location
			ee, err := localfs.NewEntry(correctSnapshotPath)
			if err != nil {
				//nolint:wrapcheck
				return nil, err
			}

			dir, ok := ee.(fs.Directory)
			if !ok {
				return nil, errors.Errorf("Filesystem snapshot path should be a directory: %v", correctSnapshotPath)
			}

			return &snapshotDirectory{
				snapshotEntry: snapshotEntry{
					snapshoter:   s.snapshoter,
					backuper:     s.backuper,
					originalRoot: originalPath,
					snapshotRoot: correctSnapshotPath,
					Entry:        ee,
				},
				directory: dir,
			}, nil
		}

		return &snapshotDirectory{
			snapshotEntry: snapshotEntry{
				snapshoter:   s.snapshoter,
				backuper:     s.backuper,
				originalRoot: s.originalRoot,
				snapshotRoot: s.snapshotRoot,
				Entry:        e,
			},
			directory: ee,
		}, nil

	case fs.File:
		return &snapshotFile{
			snapshotEntry: snapshotEntry{
				snapshoter:   s.snapshoter,
				backuper:     s.backuper,
				originalRoot: s.originalRoot,
				snapshotRoot: s.snapshotRoot,
				Entry:        e,
			},
			file: ee,
		}, nil

	case fs.Symlink:
		return &snapshotSymlink{
			snapshotEntry: snapshotEntry{
				snapshoter:   s.snapshoter,
				backuper:     s.backuper,
				originalRoot: s.originalRoot,
				snapshotRoot: s.snapshotRoot,
				Entry:        e,
			},
			symlink: ee,
		}, nil

	case fs.ErrorEntry:
		return e, nil

	default:
		return nil, errors.Errorf("Unknown entry: %t %v", e, e)
	}
}

func (s *snapshotDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	e, err := s.directory.Child(ctx, name)
	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	return s.wrapEntry(e)
}

func (s *snapshotDirectory) IterateEntries(ctx context.Context, cb func(context.Context, fs.Entry) error) error {
	//nolint:wrapcheck
	return s.directory.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		ee, err := s.wrapEntry(e)
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
