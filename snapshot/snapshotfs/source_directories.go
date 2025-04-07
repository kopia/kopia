package snapshotfs

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

type sourceDirectories struct {
	rep      repo.Repository
	userHost string
	name     string
}

func (s *sourceDirectories) IsDir() bool {
	return true
}

func (s *sourceDirectories) Name() string {
	return s.name
}

func (s *sourceDirectories) Mode() os.FileMode {
	return 0o555 | os.ModeDir //nolint:mnd
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

func (s *sourceDirectories) SupportsMultipleIterations() bool {
	return true
}

func (s *sourceDirectories) Close() {
}

func (s *sourceDirectories) Child(ctx context.Context, name string) (fs.Entry, error) {
	//nolint:wrapcheck
	return fs.IterateEntriesAndFindChild(ctx, s, name)
}

func (s *sourceDirectories) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	sources0, err := snapshot.ListSources(ctx, s.rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list sources")
	}

	// step 1 - filter sources.
	var sources []snapshot.SourceInfo

	for _, src := range sources0 {
		if src.UserName+"@"+src.Host != s.userHost {
			continue
		}

		sources = append(sources, src)
	}

	// step 2 - compute safe name for each path
	name2safe := map[string]string{}

	for _, src := range sources {
		name2safe[src.Path] = safeNameForMount(src.Path)
	}

	name2safe = disambiguateSafeNames(name2safe)

	var entries []fs.Entry

	for _, src := range sources {
		entries = append(entries, &sourceSnapshots{s.rep, src, name2safe[src.Path]})
	}

	return fs.StaticIterator(entries, nil), nil
}

func disambiguateSafeNames(m map[string]string) map[string]string {
	safe2original := map[string][]string{}

	for name, safe := range m {
		l := strings.ToLower(safe)

		// make sure we disambiguate in the lowercase space, so that both case sensitive and case-insensitive
		// filesystems will be covered.
		safe2original[l] = append(safe2original[l], name)
	}

	result := map[string]string{}
	hasAny := false

	for _, originals := range safe2original {
		if len(originals) == 1 {
			result[originals[0]] = m[originals[0]]
		} else {
			// more than 1 path map to the same path, append .1, .2, and so on in deterministic order
			sort.Strings(originals)

			for i, orig := range originals {
				if i > 0 {
					result[orig] += fmt.Sprintf("%v (%v)", m[orig], i+1)
				} else {
					result[orig] += m[orig]
				}
			}

			hasAny = true
		}
	}

	if !hasAny {
		return result
	}

	// we could have just produced some newly ambiguous names, resolve again.
	return disambiguateSafeNames(result)
}

func safeNameForMount(p string) string {
	if p == "/" {
		return "__root"
	}

	// on Windows : is not allowed, c:/ => c_ and c:\ => c_
	p = strings.ReplaceAll(p, ":/", "_")
	p = strings.ReplaceAll(p, ":\\", "_")
	p = strings.TrimLeft(p, "/")
	p = strings.ReplaceAll(p, "/", "_")
	p = strings.ReplaceAll(p, "\\", "_")
	p = strings.TrimRight(p, "_")
	p = strings.TrimRight(p, ":")

	return p
}

var _ fs.Directory = (*sourceDirectories)(nil)
