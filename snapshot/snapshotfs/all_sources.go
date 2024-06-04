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

type repositoryAllSources struct {
	rep repo.Repository
}

func (s *repositoryAllSources) IsDir() bool {
	return true
}

func (s *repositoryAllSources) Name() string {
	return "/"
}

func (s *repositoryAllSources) ModTime() time.Time {
	return s.rep.Time()
}

func (s *repositoryAllSources) Mode() os.FileMode {
	return 0o555 | os.ModeDir //nolint:mnd
}

func (s *repositoryAllSources) Size() int64 {
	return 0
}

func (s *repositoryAllSources) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (s *repositoryAllSources) Device() fs.DeviceInfo {
	return fs.DeviceInfo{}
}

func (s *repositoryAllSources) Sys() interface{} {
	return nil
}

func (s *repositoryAllSources) LocalFilesystemPath() string {
	return ""
}

func (s *repositoryAllSources) SupportsMultipleIterations() bool {
	return true
}

func (s *repositoryAllSources) Close() {
}

func (s *repositoryAllSources) Child(ctx context.Context, name string) (fs.Entry, error) {
	//nolint:wrapcheck
	return fs.IterateEntriesAndFindChild(ctx, s, name)
}

func (s *repositoryAllSources) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	srcs, err := snapshot.ListSources(ctx, s.rep)
	if err != nil {
		return nil, errors.Wrap(err, "error listing sources")
	}

	users := map[string]bool{}
	for _, src := range srcs {
		users[fmt.Sprintf("%v@%v", src.UserName, src.Host)] = true
	}

	// step 2 - compute safe name for each path
	name2safe := map[string]string{}

	for u := range users {
		name2safe[u] = safeNameForMount(u)
	}

	name2safe = disambiguateSafeNames(name2safe)

	var entries []fs.Entry

	for u := range users {
		entries = append(entries, &sourceDirectories{
			rep:      s.rep,
			userHost: u,
			name:     name2safe[u],
		})
	}

	return fs.StaticIterator(entries, nil), nil
}

// AllSourcesEntry returns fs.Directory that contains the list of all snapshot sources found in the repository.
func AllSourcesEntry(rep repo.Repository) fs.Directory {
	return &repositoryAllSources{rep: rep}
}
