package snapshotfs

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type repositoryAllSources struct {
	rep *repo.Repository
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
	srcs, err := snapshot.ListSources(ctx, s.rep)
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
			rep:      s.rep,
			userHost: u,
		})
	}

	result.Sort()
	return result, nil
}

// AllSourcesEntry returns fs.Directory that contains the list of all snapshot sources found in the repository.
func AllSourcesEntry(rep *repo.Repository) fs.Directory {
	return &repositoryAllSources{rep: rep}
}
