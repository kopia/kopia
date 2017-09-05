package snapshot

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

type repositoryAllSources struct {
	repo            *repo.Repository
	snapshotManager *Manager
}

func (s *repositoryAllSources) Parent() fs.Directory {
	return nil
}

func (s *repositoryAllSources) Metadata() *fs.EntryMetadata {
	return &fs.EntryMetadata{
		Name:        "/",
		Permissions: 0555,
		Type:        fs.EntryTypeDirectory,
		ModTime:     time.Now(),
	}
}

func (s *repositoryAllSources) Readdir() (fs.Entries, error) {
	srcs, err := s.snapshotManager.ListSources()
	if err != nil {
		return nil, err
	}

	users := map[string]bool{}
	for _, src := range srcs {
		users[fmt.Sprintf("%v@%v", src.UserName, src.Host)] = true
	}

	var result fs.Entries
	for u := range users {
		result = append(result, &sourceDirectories{
			parent:          s,
			repo:            s.repo,
			snapshotManager: s.snapshotManager,
			userHost:        u,
		})
	}

	result.Sort()
	return result, nil
}

// AllSourcesEntry returns fs.Directory that contains the list of all snapshot sources found in the repository.
func (m *Manager) AllSourcesEntry() fs.Directory {
	return &repositoryAllSources{repo: m.repository, snapshotManager: m}
}
