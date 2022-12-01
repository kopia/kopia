package snapshot_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
)

func TestSnapshotsAPI(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src1 := snapshot.SourceInfo{
		Host:     "host-1",
		UserName: "user-1",
		Path:     "/some/path",
	}

	src2 := snapshot.SourceInfo{
		Host:     "host-1",
		UserName: "user-1",
		Path:     "/some/other/path",
	}

	if _, err := snapshot.LoadSnapshot(ctx, env.RepositoryWriter, "no-such-manifest-id"); !errors.Is(err, snapshot.ErrSnapshotNotFound) {
		t.Errorf("unexpected error when deleting snapshot for manifest that does not exist: %v", err)
	}

	verifySnapshotManifestIDs(t, env.RepositoryWriter, nil, nil)
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src1, nil)
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src2, nil)
	verifyListSnapshots(t, env.RepositoryWriter, src1, []*snapshot.Manifest{})
	verifyListSnapshots(t, env.RepositoryWriter, src2, []*snapshot.Manifest{})

	manifest1 := &snapshot.Manifest{
		Source:      src1,
		Description: "some-description",
	}

	id1 := mustSaveSnapshot(t, env.RepositoryWriter, manifest1)
	verifySnapshotManifestIDs(t, env.RepositoryWriter, nil, []manifest.ID{id1})
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src1, []manifest.ID{id1})
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src2, nil)
	verifyListSnapshots(t, env.RepositoryWriter, src1, []*snapshot.Manifest{manifest1})

	manifest2 := &snapshot.Manifest{
		Source:      src1,
		Description: "some-other-description",
	}

	id2 := mustSaveSnapshot(t, env.RepositoryWriter, manifest2)
	if id1 == id2 {
		t.Errorf("expected different manifest IDs, got same: %v", id1)
	}

	verifySnapshotManifestIDs(t, env.RepositoryWriter, nil, []manifest.ID{id1, id2})
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src1, []manifest.ID{id1, id2})
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src2, nil)

	manifest3 := &snapshot.Manifest{
		Source:      src2,
		Description: "some-other-description",
	}

	id3 := mustSaveSnapshot(t, env.RepositoryWriter, manifest3)
	verifySnapshotManifestIDs(t, env.RepositoryWriter, nil, []manifest.ID{id1, id2, id3})
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src1, []manifest.ID{id1, id2})
	verifySnapshotManifestIDs(t, env.RepositoryWriter, &src2, []manifest.ID{id3})
	verifySources(t, env.RepositoryWriter, src1, src2)
	verifyLoadSnapshots(t, env.RepositoryWriter, []manifest.ID{id1, id2, id3}, []*snapshot.Manifest{manifest1, manifest2, manifest3})

	require.True(t, manifest3.UpdatePins([]string{"new-pin"}, nil))
	require.NoError(t, snapshot.UpdateSnapshot(ctx, env.RepositoryWriter, manifest3))

	require.NotEqual(t, manifest3.ID, id3)

	updated3, err := snapshot.LoadSnapshot(ctx, env.RepositoryWriter, manifest3.ID)
	require.NoError(t, err)
	require.Equal(t, updated3, manifest3)
}

func verifySnapshotManifestIDs(t *testing.T, rep repo.Repository, src *snapshot.SourceInfo, expected []manifest.ID) {
	t.Helper()

	res, err := snapshot.ListSnapshotManifests(testlogging.Context(t), rep, src, nil)
	if err != nil {
		t.Errorf("error listing snapshot manifests: %v", err)
	}

	sortManifestIDs(res)
	sortManifestIDs(expected)

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("unexpected manifests: %v, wanted %v", res, expected)
	}
}

func sortManifestIDs(s []manifest.ID) {
	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})
}

func mustSaveSnapshot(t *testing.T, rep repo.RepositoryWriter, man *snapshot.Manifest) manifest.ID {
	t.Helper()

	id, err := snapshot.SaveSnapshot(testlogging.Context(t), rep, man)
	if err != nil {
		t.Fatalf("error saving snapshot: %v", err)
	}

	return id
}

func verifySources(t *testing.T, rep repo.Repository, sources ...snapshot.SourceInfo) {
	t.Helper()

	actualSources, err := snapshot.ListSources(testlogging.Context(t), rep)
	if err != nil {
		t.Errorf("error listing sources: %v", err)
	}

	if got, want := sorted(sourcesToStrings(actualSources...)), sorted(sourcesToStrings(sources...)); !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected sources: %v want %v", got, want)
	}
}

func verifyListSnapshots(t *testing.T, rep repo.Repository, src snapshot.SourceInfo, expected []*snapshot.Manifest) {
	t.Helper()

	got, err := snapshot.ListSnapshots(testlogging.Context(t), rep, src)
	if err != nil {
		t.Errorf("error loading manifests: %v", err)
		return
	}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("unexpected manifests: %v, wanted %v", got, expected)

		for i, m := range got {
			t.Logf("got[%v]=%#v", i, m)
		}

		for i, m := range expected {
			t.Logf("want[%v]=%#v", i, m)
		}
	}
}

func verifyLoadSnapshots(t *testing.T, rep repo.Repository, ids []manifest.ID, expected []*snapshot.Manifest) {
	t.Helper()

	got, err := snapshot.LoadSnapshots(testlogging.Context(t), rep, ids)
	if err != nil {
		t.Errorf("error loading manifests: %v", err)
		return
	}

	if !reflect.DeepEqual(got, expected) {
		t.Errorf("unexpected manifests: %v, wanted %v", got, expected)

		for i, m := range got {
			t.Logf("got[%v]=%#v", i, m)
		}

		for i, m := range expected {
			t.Logf("want[%v]=%#v", i, m)
		}
	}
}

func sorted(s []string) []string {
	res := append([]string(nil), s...)
	sort.Strings(res)

	return res
}

func sourcesToStrings(sources ...snapshot.SourceInfo) []string {
	var res []string

	for _, src := range sources {
		res = append(res, src.String())
	}

	return res
}

func mustAbs(t *testing.T, p string) string {
	t.Helper()

	p2, err := filepath.Abs(p)
	require.NoError(t, err)

	return p2
}

func TestParseSourceInfo(t *testing.T) {
	cwd, _ := os.Getwd()

	cases := []struct {
		path string
		want snapshot.SourceInfo
	}{
		{"(global)", snapshot.SourceInfo{}},
		{"", snapshot.SourceInfo{UserName: "default-user", Host: "default-host", Path: cwd}},
		{".", snapshot.SourceInfo{UserName: "default-user", Host: "default-host", Path: cwd}},
		{"..", snapshot.SourceInfo{UserName: "default-user", Host: "default-host", Path: filepath.Clean(filepath.Join(cwd, ".."))}},
		{"foo@bar:/some/path", snapshot.SourceInfo{UserName: "foo", Host: "bar", Path: "/some/path"}},
		{"/some/path", snapshot.SourceInfo{UserName: "default-user", Host: "default-host", Path: mustAbs(t, "/some/path")}},
		{"/some/path/../other-path", snapshot.SourceInfo{UserName: "default-user", Host: "default-host", Path: mustAbs(t, "/some/other-path")}},
		{"@some-host", snapshot.SourceInfo{Host: "some-host"}},
		{"some-user@some-host", snapshot.SourceInfo{UserName: "some-user", Host: "some-host"}},
	}

	for _, tc := range cases {
		got, err := snapshot.ParseSourceInfo(tc.path, "default-host", "default-user")
		if err != nil {
			t.Errorf("error parsing %q: %v", tc.path, err)
			continue
		}

		if got != tc.want {
			t.Errorf("unexpected parsed value of %q: %v, wanted %v", tc.path, got, tc.want)
		}

		got2, err := snapshot.ParseSourceInfo(got.String(), "default-host", "default-user")
		if err != nil {
			t.Errorf("error parsing %q: %v", tc.path, err)
			continue
		}

		if got != got2 {
			t.Errorf("unexpected parsed value of %q: %v, wanted %v", got.String(), got2, got)
		}
	}
}

func TestParseInvalidSourceInfo(t *testing.T) {
	cases := []string{
		"@",
	}

	for _, tc := range cases {
		si, err := snapshot.ParseSourceInfo(tc, "default-host", "default-user")
		if err == nil {
			t.Errorf("unexpected success when parsing %v: %v", tc, si)
		}
	}
}

func TestUpdatePins(t *testing.T) {
	m := snapshot.Manifest{}

	require.True(t, m.UpdatePins([]string{"d", "c", "b"}, nil))
	require.False(t, m.UpdatePins([]string{"d", "c", "b"}, nil))
	require.Equal(t, []string{"b", "c", "d"}, m.Pins) // pins are sorted

	require.True(t, m.UpdatePins([]string{"e", "a"}, []string{"c"}))
	require.False(t, m.UpdatePins([]string{"e", "a"}, []string{"c"}))
	require.Equal(t, []string{"a", "b", "d", "e"}, m.Pins)
}
