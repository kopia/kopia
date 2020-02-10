package snapshot_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
)

func TestSnapshotsAPI(t *testing.T) {
	var env repotesting.Environment
	defer env.Setup(t).Close(t)

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

	verifySnapshotManifestIDs(t, env.Repository, nil, nil)
	verifySnapshotManifestIDs(t, env.Repository, &src1, nil)
	verifySnapshotManifestIDs(t, env.Repository, &src2, nil)
	verifyListSnapshots(t, env.Repository, src1, []*snapshot.Manifest{})
	verifyListSnapshots(t, env.Repository, src2, []*snapshot.Manifest{})

	manifest1 := &snapshot.Manifest{
		Source:      src1,
		Description: "some-description",
	}

	id1 := mustSaveSnapshot(t, env.Repository, manifest1)
	verifySnapshotManifestIDs(t, env.Repository, nil, []manifest.ID{id1})
	verifySnapshotManifestIDs(t, env.Repository, &src1, []manifest.ID{id1})
	verifySnapshotManifestIDs(t, env.Repository, &src2, nil)
	verifyListSnapshots(t, env.Repository, src1, []*snapshot.Manifest{manifest1})

	manifest2 := &snapshot.Manifest{
		Source:      src1,
		Description: "some-other-description",
	}

	id2 := mustSaveSnapshot(t, env.Repository, manifest2)
	if id1 == id2 {
		t.Errorf("expected different manifest IDs, got same: %v", id1)
	}

	verifySnapshotManifestIDs(t, env.Repository, nil, []manifest.ID{id1, id2})
	verifySnapshotManifestIDs(t, env.Repository, &src1, []manifest.ID{id1, id2})
	verifySnapshotManifestIDs(t, env.Repository, &src2, nil)

	manifest3 := &snapshot.Manifest{
		Source:      src2,
		Description: "some-other-description",
	}

	id3 := mustSaveSnapshot(t, env.Repository, manifest3)
	verifySnapshotManifestIDs(t, env.Repository, nil, []manifest.ID{id1, id2, id3})
	verifySnapshotManifestIDs(t, env.Repository, &src1, []manifest.ID{id1, id2})
	verifySnapshotManifestIDs(t, env.Repository, &src2, []manifest.ID{id3})
	verifySources(t, env.Repository, src1, src2)
	verifyLoadSnapshots(t, env.Repository, []manifest.ID{id1, id2, id3}, []*snapshot.Manifest{manifest1, manifest2, manifest3})
}

func verifySnapshotManifestIDs(t *testing.T, rep *repo.Repository, src *snapshot.SourceInfo, expected []manifest.ID) {
	t.Helper()

	res, err := snapshot.ListSnapshotManifests(context.Background(), rep, src)
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

func mustSaveSnapshot(t *testing.T, rep *repo.Repository, man *snapshot.Manifest) manifest.ID {
	t.Helper()

	id, err := snapshot.SaveSnapshot(context.Background(), rep, man)
	if err != nil {
		t.Fatalf("error saving snapshot: %v", err)
	}

	return id
}

func verifySources(t *testing.T, rep *repo.Repository, sources ...snapshot.SourceInfo) {
	actualSources, err := snapshot.ListSources(context.Background(), rep)
	if err != nil {
		t.Errorf("error listing sources: %v", err)
	}

	if got, want := sorted(sourcesToStrings(actualSources...)), sorted(sourcesToStrings(sources...)); !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected sources: %v want %v", got, want)
	}
}

func verifyListSnapshots(t *testing.T, rep *repo.Repository, src snapshot.SourceInfo, expected []*snapshot.Manifest) {
	t.Helper()

	got, err := snapshot.ListSnapshots(context.Background(), rep, src)
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

func verifyLoadSnapshots(t *testing.T, rep *repo.Repository, ids []manifest.ID, expected []*snapshot.Manifest) {
	got, err := snapshot.LoadSnapshots(context.Background(), rep, ids)
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
	p2, err := filepath.Abs(p)
	if err != nil {
		t.Fatal(err)
	}

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
