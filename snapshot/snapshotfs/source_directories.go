package snapshotfs

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type sourceDirectories struct {
	rep      repo.Repository
	userHost string
}

func (s *sourceDirectories) IsDir() bool {
	return true
}

func (s *sourceDirectories) Name() string {
	return s.userHost
}

func (s *sourceDirectories) Mode() os.FileMode {
	return 0o555 | os.ModeDir
}

func (s *sourceDirectories) ModTime() time.Time {
	return s.rep.Time()
}

func (s *sourceDirectories) Sys() interface{} {
	return nil
}

func (s *sourceDirectories) Size() int64 {
	return 0
}

func (s *sourceDirectories) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (s *sourceDirectories) Device() fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func (s *sourceDirectories) LocalFilesystemPath() string {
	return ""
}

func (s *sourceDirectories) Child(ctx context.Context, name string) (fs.Entry, error) {
	return fs.ReadDirAndFindChild(ctx, s, name)
}

func (s *sourceDirectories) Readdir(ctx context.Context) (fs.Entries, error) {
	sources, err := snapshot.ListSources(ctx, s.rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list sources")
	}

	var result fs.Entries

	for _, src := range sources {
		if src.UserName+"@"+src.Host != s.userHost {
			continue
		}

		result = append(result, &sourceSnapshots{s.rep, src})
	}

	result.Sort()

	return result, nil
}

var _ fs.Directory = (*sourceDirectories)(nil)
