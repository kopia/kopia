package snapshot

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/dir"
	"github.com/kopia/kopia/repo"
)

type sourceSnapshots struct {
	parent          fs.Directory
	repo            *repo.Repository
	snapshotManager *Manager
	src             *SourceInfo
}

func (s *sourceSnapshots) Parent() fs.Directory {
	return s.parent
}

func (s *sourceSnapshots) Metadata() *fs.EntryMetadata {
	return &fs.EntryMetadata{
		Name:        fmt.Sprintf("%v", safeName(s.src.Path)),
		Permissions: 0555,
		Type:        fs.EntryTypeDirectory,
	}
}

func safeName(path string) string {
	path = strings.TrimLeft(path, "/")
	return strings.Replace(path, "/", "_", -1)
}

func (s *sourceSnapshots) Readdir() (fs.Entries, error) {
	manifests, err := s.snapshotManager.ListSnapshots(*s.src)
	if err != nil {
		return nil, err
	}

	var result fs.Entries

	for _, m := range manifests {
		name := m.StartTime.Format("20060102-150405")
		if m.IncompleteReason != "" {
			name += fmt.Sprintf(" (%v)", m.IncompleteReason)
		}
		e := newRepoEntry(s.repo, &dir.Entry{
			EntryMetadata: fs.EntryMetadata{
				Name:        name,
				Permissions: 0555,
				Type:        fs.EntryTypeDirectory,
				ModTime:     m.StartTime,
			},
			ObjectID: m.RootObjectID,
		}, s)

		result = append(result, e)
	}
	result.Sort()

	return result, nil
}
