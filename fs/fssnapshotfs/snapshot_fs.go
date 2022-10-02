package fssnapshotfs

import (
	"context"
	"fmt"
	"path/filepath"

	fs_snapshot "github.com/pescuma/go-fs-snapshot/lib"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type snapshotEntry struct {
	policyTree      *policy.Tree
	snapshoter      fs_snapshot.Snapshoter
	backuper        fs_snapshot.Backuper
	closeSnapshoter bool
	originalRoot    string
	snapshotRoot    string
	fs.Entry
}

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
func NewEntry(originalPath string, policyTree *policy.Tree) (fs.Entry, error) {
	entry, err := localfs.NewEntry(originalPath)
	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	cfg := &fs_snapshot.SnapshoterConfig{
		InfoCallback: func(level fs_snapshot.MessageLevel, msg string, a ...interface{}) {
			if level == fs_snapshot.OutputLevel {
				fmt.Printf(msg+"\n", a...)
			}
		},
	}

	snapshoter, err := fs_snapshot.NewSnapshoter(cfg)
	if err != nil {
		// If we can't use snapshots, just use the filesystem directly
		fmt.Printf("Error creating filesystem snapshot: %v\n", err)

		return entry, nil
	}

	backuper, err := snapshoter.StartBackup(nil)
	if err != nil {
		// If we can't use snapshots, just use the filesystem directly
		fmt.Printf("Error creating filesystem snapshot: %v\n", err)

		snapshoter.Close()

		return entry, nil
	}

	se := &snapshotEntry{
		policyTree:   policyTree,
		snapshoter:   snapshoter,
		backuper:     backuper,
		originalRoot: originalPath,
		snapshotRoot: originalPath,
		Entry:        entry,
	}

	return se.wrapEntry(entry, true)
}

func createFilesystemSnapshot(backuper fs_snapshot.Backuper, policyTree *policy.Tree, originalPath string) string {
	if !policyTree.EffectivePolicy().FilesPolicy.UseFsSnapshots.OrDefault(false) {
		return originalPath
	}

	snapshotPath, err := backuper.TryToCreateTemporarySnapshot(originalPath)
	if err != nil {
		if err.Error() != "snapshot failed in a previous attempt" {
			fmt.Printf("Error creating filesystem snapshot for '%v': %v. Ignoring and using original files.", originalPath, err.Error())
		}

		// On case of error just use original dir
		return originalPath
	}

	if snapshotPath[len(snapshotPath)-1] == filepath.Separator {
		snapshotPath = snapshotPath[:len(snapshotPath)-1]
	}

	return snapshotPath
}

func (s *snapshotEntry) LocalFilesystemPath() string {
	return s.toOriginal(s.Entry.LocalFilesystemPath())
}

func (s *snapshotEntry) hasSnapshot() bool {
	return s.originalRoot != s.snapshotRoot
}

func (s *snapshotEntry) toOriginal(snapshotPath string) string {
	if !s.hasSnapshot() {
		return snapshotPath
	}

	relative, err := filepath.Rel(s.snapshotRoot, snapshotPath)
	if err != nil {
		// Should never happen
		panic(err)
	}

	return filepath.Join(s.originalRoot, relative)
}

func (s *snapshotEntry) wrapEntry(e fs.Entry, isRoot bool) (fs.Entry, error) {
	switch et := e.(type) {
	case fs.Directory:
		policyTree := s.policyTree
		if !isRoot {
			policyTree = s.policyTree.Child(e.Name())
		}

		originalRoot := s.originalRoot
		snapshotRoot := s.snapshotRoot

		snapshotPath := e.LocalFilesystemPath()
		originalPath := s.toOriginal(snapshotPath)
		correctSnapshotPath := createFilesystemSnapshot(s.backuper, policyTree, originalPath)

		if correctSnapshotPath != snapshotPath {
			// Changed snapshot location
			en, err := localfs.NewEntry(correctSnapshotPath)
			if err != nil {
				//nolint:wrapcheck
				return nil, err
			}

			dir, ok := en.(fs.Directory)
			if !ok {
				return nil, errors.Errorf("should be a directory: %v", correctSnapshotPath)
			}

			originalRoot = originalPath
			snapshotRoot = correctSnapshotPath
			e = en
			et = dir
		}

		return &snapshotDirectory{
			snapshotEntry: snapshotEntry{
				policyTree:      policyTree,
				snapshoter:      s.snapshoter,
				backuper:        s.backuper,
				closeSnapshoter: isRoot,
				originalRoot:    originalRoot,
				snapshotRoot:    snapshotRoot,
				Entry:           e,
			},
			directory: et,
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
			file: et,
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
			symlink: et,
		}, nil

	case fs.ErrorEntry:
		return e, nil

	default:
		return nil, errors.Errorf("Unknown entry: %t %v", e, e)
	}
}

func (s *snapshotEntry) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	if defp, ok := s.Entry.(snapshot.HasDirEntryOrNil); ok {
		//nolint:wrapcheck
		return defp.DirEntryOrNil(ctx)
	}

	return nil, nil
}

func (s *snapshotEntry) Close() {
	s.Entry.Close()

	if s.closeSnapshoter {
		s.backuper.Close()
		s.snapshoter.Close()
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

	return s.wrapEntry(e, false)
}

func (s *snapshotDirectory) IterateEntries(ctx context.Context, cb func(context.Context, fs.Entry) error) error {
	//nolint:wrapcheck
	return s.directory.IterateEntries(ctx, func(ctx context.Context, e fs.Entry) error {
		ee, err := s.wrapEntry(e, false)
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
