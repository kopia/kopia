package repofs

import (
	"context"
	"time"

	"github.com/kopia/kopia/snapshot"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

type sourceDirectories struct {
	rep      *repo.Repository
	userHost string
}

func (s *sourceDirectories) Metadata() *fs.EntryMetadata {
	return &fs.EntryMetadata{
		Name:        s.userHost,
		Permissions: 0555,
		Type:        fs.EntryTypeDirectory,
		ModTime:     time.Now(),
	}
}

func (s *sourceDirectories) Summary() *fs.DirectorySummary {
	return nil
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
