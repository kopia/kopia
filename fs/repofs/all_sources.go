package repofs

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

type repositoryAllSources struct {
	snapshotManager *snapshot.Manager
}

func (s *repositoryAllSources) Summary() *fs.DirectorySummary {
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

func (s *repositoryAllSources) Readdir(ctx context.Context) (fs.Entries, error) {
	srcs := s.snapshotManager.ListSources()

	users := map[string]bool{}
	for _, src := range srcs {
		users[fmt.Sprintf("%v@%v", src.UserName, src.Host)] = true
	}

	var result fs.Entries
	for u := range users {
		result = append(result, &sourceDirectories{
			snapshotManager: s.snapshotManager,
			userHost:        u,
		})
	}

	result.Sort()
	return result, nil
}

// AllSourcesEntry returns fs.Directory that contains the list of all snapshot sources found in the repository.
func AllSourcesEntry(m *snapshot.Manager) fs.Directory {
	return &repositoryAllSources{snapshotManager: m}
}
