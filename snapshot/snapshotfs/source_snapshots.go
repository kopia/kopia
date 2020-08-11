package snapshotfs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type sourceSnapshots struct {
	rep repo.Repository
	src snapshot.SourceInfo
}

func (s *sourceSnapshots) IsDir() bool {
	return true
}

func (s *sourceSnapshots) Name() string {
	return fmt.Sprintf("%v", safeName(s.src.Path))
}

func (s *sourceSnapshots) Mode() os.FileMode {
	return 0o555 | os.ModeDir
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

func safeName(path string) string {
	path = strings.TrimLeft(path, "/")
	return strings.Replace(path, "/", "_", -1)
}

func (s *sourceSnapshots) Summary() *fs.DirectorySummary {
	return nil
}

func (s *sourceSnapshots) Child(ctx context.Context, name string) (fs.Entry, error) {
	return fs.ReadDirAndFindChild(ctx, s, name)
}

func (s *sourceSnapshots) Readdir(ctx context.Context) (fs.Entries, error) {
	manifests, err := snapshot.ListSnapshots(ctx, s.rep, s.src)
	if err != nil {
		return nil, err
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

		e, err := EntryFromDirEntry(s.rep, de)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create entry")
		}

		result = append(result, e)
	}

	result.Sort()

	return result, nil
}

var _ fs.Directory = (*sourceSnapshots)(nil)
