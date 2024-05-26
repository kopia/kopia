package snapshotfs

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type sourceSnapshots struct {
	rep  repo.Repository
	src  snapshot.SourceInfo
	name string
}

func (s *sourceSnapshots) IsDir() bool {
	return true
}

func (s *sourceSnapshots) Name() string {
	return s.name
}

func (s *sourceSnapshots) Mode() os.FileMode {
	return 0o555 | os.ModeDir //nolint:mnd
}

func (s *sourceSnapshots) Size() int64 {
	return 0
}

func (s *sourceSnapshots) Sys() interface{} {
	return nil
}

func (s *sourceSnapshots) ModTime() time.Time {
	return s.rep.Time()
}

func (s *sourceSnapshots) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (s *sourceSnapshots) Device() fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func (s *sourceSnapshots) LocalFilesystemPath() string {
	return ""
}

func (s *sourceSnapshots) SupportsMultipleIterations() bool {
	return true
}

func (s *sourceSnapshots) Close() {
}

func (s *sourceSnapshots) Child(ctx context.Context, name string) (fs.Entry, error) {
	//nolint:wrapcheck
	return fs.IterateEntriesAndFindChild(ctx, s, name)
}

func (s *sourceSnapshots) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	manifests, err := snapshot.ListSnapshots(ctx, s.rep, s.src)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list snapshots")
	}

	var entries []fs.Entry

	for _, m := range manifests {
		name := m.StartTime.Format("20060102-150405")
		if m.IncompleteReason != "" {
			name += fmt.Sprintf(" (%v)", m.IncompleteReason)
		}

		de := &snapshot.DirEntry{
			Name:        name,
			Permissions: 0o555, //nolint:mnd
			Type:        snapshot.EntryTypeDirectory,
			ModTime:     m.StartTime,
			ObjectID:    m.RootObjectID(),
		}

		if m.RootEntry != nil {
			de.DirSummary = m.RootEntry.DirSummary
		}

		entries = append(entries, EntryFromDirEntry(s.rep, de))
	}

	return fs.StaticIterator(entries, nil), nil
}

var _ fs.Directory = (*sourceSnapshots)(nil)
