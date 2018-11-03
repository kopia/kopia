package snapshotfs

import (
	"context"
	"os"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/repo"
)

type sourceDirectories struct {
	rep      *repo.Repository
	userHost string
}

func (s *sourceDirectories) IsDir() bool {
	return true
}

func (s *sourceDirectories) Name() string {
	return s.userHost
}

func (s *sourceDirectories) Mode() os.FileMode {
	return 0555 | os.ModeDir
}

func (s *sourceDirectories) ModTime() time.Time {
	return time.Now()
}

func (s *sourceDirectories) Sys() interface{} {
	return nil
}

func (s *sourceDirectories) Summary() *fs.DirectorySummary {
	return nil
}

func (s *sourceDirectories) Size() int64 {
	return 0
}

func (s *sourceDirectories) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (s *sourceDirectories) Readdir(ctx context.Context) (fs.Entries, error) {
	sources, err := snapshot.ListSources(ctx, s.rep)
	if err != nil {
		return nil, err
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
