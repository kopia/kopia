package snapshotfs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

func TestAllSources(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	u := NewUploader(env.RepositoryWriter)
	man, err := u.Upload(ctx, mockfs.NewDirectory(), nil, snapshot.SourceInfo{Host: "dummy", UserName: "dummy", Path: "dummy"})
	require.NoError(t, err)

	manifests := []struct {
		user, host, path, timestamp string
	}{
		{"some-user", "some-host", "c:/some/path", "2020-01-01T12:01:03Z"},
		{"some-user", "some-host", "c:/some/path", "2020-01-01T12:01:04Z"},
		{"some-user", "some-host", "c:/some/path", "2020-01-01T12:01:05Z"},
		// uppercase version of the previous one
		{"some-user", "some-host", "c:/some/Path", "2020-01-01T12:01:03Z"},
		// make sure we cleanup names even for usernames and hostnames, even though special characters should not be allowed
		{"some/user", "some-host", "c:/some/path", "2020-01-01T13:01:03Z"},
		{"some/user", "some-host", "c:/some/path", "2020-01-01T13:01:04Z"},
		{"some\\user", "some-host", "c:/some/path", "2020-01-01T14:01:03Z"},
		{"some\\user", "some-host", "c:/some/path", "2020-01-01T14:01:04Z"},
		// root
		{"another-user", "some-host", "/", "2020-01-01T12:01:03Z"},
		{"another-user", "some-host", "/tmp", "2020-01-01T12:01:03Z"},
		{"another-user", "some-host", "/var", "2020-01-01T12:01:03Z"},
	}

	for _, m := range manifests {
		ts, err := time.Parse(time.RFC3339, m.timestamp)
		require.NoError(t, err)

		mustWriteSnapshotManifest(ctx, t, env.RepositoryWriter, snapshot.SourceInfo{UserName: m.user, Host: m.host, Path: m.path}, fs.UTCTimestampFromTime(ts), man)
	}

	as := AllSourcesEntry(env.RepositoryWriter)
	gotNames := iterateAllNames(ctx, t, as, "")
	wantNames := map[string]struct{}{
		"another-user@some-host/":                              {},
		"another-user@some-host/__root/":                       {},
		"another-user@some-host/__root/20200101-120103/":       {},
		"another-user@some-host/tmp/":                          {},
		"another-user@some-host/tmp/20200101-120103/":          {},
		"another-user@some-host/var/":                          {},
		"another-user@some-host/var/20200101-120103/":          {},
		"some-user@some-host/":                                 {},
		"some-user@some-host/c_some_Path/":                     {},
		"some-user@some-host/c_some_Path/20200101-120103/":     {},
		"some-user@some-host/c_some_path (2)/":                 {},
		"some-user@some-host/c_some_path (2)/20200101-120103/": {},
		"some-user@some-host/c_some_path (2)/20200101-120104/": {},
		"some-user@some-host/c_some_path (2)/20200101-120105/": {},
		"some_user@some-host/":                                 {},
		"some_user@some-host/c_some_path/":                     {},
		"some_user@some-host/c_some_path/20200101-130103/":     {},
		"some_user@some-host/c_some_path/20200101-130104/":     {},
		"some_user@some-host (2)/":                             {},
		"some_user@some-host (2)/c_some_path/":                 {},
		"some_user@some-host (2)/c_some_path/20200101-140103/": {},
		"some_user@some-host (2)/c_some_path/20200101-140104/": {},
	}

	require.Equal(t, wantNames, gotNames)
}

func iterateAllNames(ctx context.Context, t *testing.T, dir fs.Directory, prefix string) map[string]struct{} {
	t.Helper()

	result := map[string]struct{}{}

	err := fs.IterateEntries(ctx, dir, func(innerCtx context.Context, ent fs.Entry) error {
		if ent.IsDir() {
			result[prefix+ent.Name()+"/"] = struct{}{}
			childEntries := iterateAllNames(ctx, t, ent.(fs.Directory), prefix+ent.Name()+"/")

			for k, v := range childEntries {
				result[k] = v
			}
		} else {
			result[prefix+ent.Name()] = struct{}{}
		}

		return nil
	})
	require.NoError(t, err)

	return result
}

func mustWriteSnapshotManifest(ctx context.Context, t *testing.T, rep repo.RepositoryWriter, src snapshot.SourceInfo, startTime fs.UTCTimestamp, man *snapshot.Manifest) {
	t.Helper()

	man.Source = src
	man.StartTime = startTime

	_, err := snapshot.SaveSnapshot(ctx, rep, man)
	require.NoError(t, err)
}

func TestSafeNameForMount(t *testing.T) {
	cases := map[string]string{
		"/tmp/foo/bar":             "tmp_foo_bar",
		"/root":                    "root",
		"/root/":                   "root",
		"/":                        "__root",
		"C:":                       "C",
		"C:\\":                     "C",
		"C:\\foo":                  "C_foo",
		"C:\\foo/bar":              "C_foo_bar",
		"\\\\server\\root":         "__server_root",
		"\\\\server\\root\\":       "__server_root",
		"\\\\server\\root\\subdir": "__server_root_subdir",
		"\\\\server\\root\\subdir/with/forward/slashes":  "__server_root_subdir_with_forward_slashes",
		"\\\\server\\root\\subdir/with\\mixed/slashes\\": "__server_root_subdir_with_mixed_slashes",
	}

	for input, want := range cases {
		assert.Equal(t, want, safeNameForMount(input), input)
	}
}

func TestDisambiguateSafeNames(t *testing.T) {
	cases := []struct {
		input map[string]string
		want  map[string]string
	}{
		{
			input: map[string]string{
				"c:/":  "c",
				"c:\\": "c",
				"c:":   "c",
				"c":    "c",
			},
			want: map[string]string{
				"c":    "c",
				"c:":   "c (2)",
				"c:/":  "c (3)",
				"c:\\": "c (4)",
			},
		},
		{
			input: map[string]string{
				"c:/":   "c",
				"c:\\":  "c",
				"c:":    "c",
				"c":     "c",
				"c (2)": "c (2)",
			},
			want: map[string]string{
				"c":     "c",
				"c (2)": "c (2)",
				"c:":    "c (2) (2)",
				"c:/":   "c (3)",
				"c:\\":  "c (4)",
			},
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, disambiguateSafeNames(tc.input))
	}
}
