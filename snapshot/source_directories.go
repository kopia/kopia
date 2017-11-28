package snapshot

import (
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

type sourceDirectories struct {
	parent          fs.Directory
	repo            *repo.Repository
	snapshotManager *Manager
	userHost        string
}

func (s *sourceDirectories) Parent() fs.Directory {
	return s.parent
}

func (s *sourceDirectories) Metadata() *fs.EntryMetadata {
	return &fs.EntryMetadata{
		Name:        s.userHost,
		Permissions: 0555,
		Type:        fs.EntryTypeDirectory,
		ModTime:     time.Now(),
	}
}

func (s *sourceDirectories) Readdir() (fs.Entries, error) {
	sources := s.snapshotManager.ListSources()
	var result fs.Entries

	for _, src := range sources {
		if src.UserName+"@"+src.Host != s.userHost {
			continue
		}

		result = append(result, &sourceSnapshots{s, s.repo, s.snapshotManager, src})
	}

	result.Sort()

	return result, nil
}
