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
	return 0o555 | os.ModeDir // nolint:gomnd
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

func (s *sourceSnapshots) Child(ctx context.Context, name string) (fs.Entry, error) {
	// nolint:wrapcheck
	return fs.ReadDirAndFindChild(ctx, s, name)
}

func (s *sourceSnapshots) IterateEntries(ctx context.Context, cb func(context.Context, fs.Entry) error) error {
	return fs.ReaddirToIterate(ctx, s, cb)
}

func (s *sourceSnapshots) Readdir(ctx context.Context) (fs.Entries, error) {
	manifests, err := snapshot.ListSnapshots(ctx, s.rep, s.src)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list snapshots")
	}

	var result fs.Entries

	for _, m := range manifests {
		name := m.StartTime.Format("20060102-150405")
		if m.IncompleteReason != "" {
			name += fmt.Sprintf(" (%v)", m.IncompleteReason)
		}

		de := &snapshot.DirEntry{
			Name:        name,
			Permissions: 0o555, //nolint:gomnd
			Type:        snapshot.EntryTypeDirectory,
			ModTime:     m.StartTime,
			ObjectID:    m.RootObjectID(),
		}

		if m.RootEntry != nil {
			de.DirSummary = m.RootEntry.DirSummary
		}

		result = append(result, EntryFromDirEntry(s.rep, de))
	}

	result.Sort()

	return result, nil
}

var _ fs.Directory = (*sourceSnapshots)(nil)
