package snapshot_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
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
	require.NoError(t, err, "error listing snapshot manifests")

	sortManifestIDs(res)
	sortManifestIDs(expected)

	if !reflect.DeepEqual(res, expected) {
		t.Errorf("unexpected manifests: %v, wanted %v", res, expected)
	}
}

func sortManifestIDs(s []manifest.ID) {
	slices.Sort(s)
}

func mustSaveSnapshot(t *testing.T, rep repo.RepositoryWriter, man *snapshot.Manifest) manifest.ID {
	t.Helper()

	id, err := snapshot.SaveSnapshot(testlogging.Context(t), rep, man)
	require.NoError(t, err, "error saving snapshot")

	return id
}

func verifySources(t *testing.T, rep repo.Repository, sources ...snapshot.SourceInfo) {
	t.Helper()

	actualSources, err := snapshot.ListSources(testlogging.Context(t), rep)
	require.NoError(t, err, "error listing sources")
	require.ElementsMatch(t, sources, actualSources, "unexpected sources")
}

func verifyListSnapshots(t *testing.T, rep repo.Repository, src snapshot.SourceInfo, expected []*snapshot.Manifest) {
	t.Helper()

	got, err := snapshot.ListSnapshots(testlogging.Context(t), rep, src)
	require.NoError(t, err, "error loading manifests")
	verifyEqualManifests(t, expected, got)
}

func verifyEqualManifests(t *testing.T, expected, got []*snapshot.Manifest) {
	t.Helper()

	if !assert.Equal(t, expected, got, "unexpected manifests") {
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
	require.NoError(t, err, "error loading manifests")
	verifyEqualManifests(t, expected, got)
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
		require.NoErrorf(t, err, "error parsing %q", tc.path)
		require.Equal(t, tc.want, got, "unexpected parsed value")

		got2, err := snapshot.ParseSourceInfo(got.String(), "default-host", "default-user")
		require.NoErrorf(t, err, "error parsing %q", tc.path)
		require.Equal(t, got, got2, "unexpected parsed value")
	}
}

func TestParseInvalidSourceInfo(t *testing.T) {
	cases := []string{
		"@",
	}

	for _, tc := range cases {
		si, err := snapshot.ParseSourceInfo(tc, "default-host", "default-user")
		require.Errorf(t, err, "unexpected success when parsing %v: %v", tc, si)
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

// Helper to create a Manifest with given times.
func newManifest(start, end time.Time) *snapshot.Manifest {
	return &snapshot.Manifest{
		StartTime: fs.UTCTimestampFromTime(start),
		EndTime:   fs.UTCTimestampFromTime(end),
	}
}

func TestSortByTimeAscending(t *testing.T) {
	start0 := time.Date(2018, time.January, 10, 13, 23, 21, 0, time.UTC)
	start1 := start0.Add(3 * time.Hour)
	start2 := time.Date(2018, time.January, 10, 13, 30, 1, 0, time.UTC)
	start3 := start2
	start4 := time.Date(2018, time.January, 10, 14, 0, 1, 0, time.UTC)

	end1 := start1.Add(20 * time.Second)
	end2 := start2.Add(40 * time.Second)
	end3 := start3.Add(2 * time.Minute)
	end4 := start4.Add(5 * time.Minute)
	end0 := start0.Add(1000 * time.Hour) // overlaps with other snapshots

	manifests := []*snapshot.Manifest{
		newManifest(start2, end2),
		newManifest(start3, end3), // same start time as start2, later end time
		newManifest(start1, end1),
		newManifest(start0, end0),
		newManifest(start4, end4),
	}

	// Test normal sort (most recent last)
	sorted := snapshot.SortByTime(manifests, false)

	prev := sorted[0]
	for _, s := range sorted[1:] {
		require.LessOrEqual(t, prev.StartTime, s.StartTime, "start time is after")

		if prev.StartTime == s.StartTime {
			require.LessOrEqual(t, prev.EndTime, s.EndTime, "end time is after")
		}
	}
}

func TestSortByTimeDescending(t *testing.T) {
	start0 := time.Date(2018, time.January, 10, 13, 23, 21, 0, time.UTC)
	start1 := start0.Add(3 * time.Hour)
	start2 := time.Date(2018, time.January, 10, 13, 30, 1, 0, time.UTC)
	start3 := start2
	start4 := time.Date(2018, time.January, 10, 14, 0, 1, 0, time.UTC)

	end1 := start1.Add(20 * time.Second)
	end2 := start2.Add(40 * time.Second)
	end3 := start3.Add(2 * time.Minute)
	end4 := start4.Add(5 * time.Minute)
	end0 := start0.Add(1000 * time.Hour) // overlaps with other snapshots

	manifests := []*snapshot.Manifest{
		newManifest(start2, end2),
		newManifest(start3, end3), // same start time as start2, later end time
		newManifest(start1, end1),
		newManifest(start0, end0),
		newManifest(start4, end4),
	}

	// Test reverse sort (most recent first)
	sorted := snapshot.SortByTime(manifests, true)

	prev := sorted[0]
	for _, s := range sorted[1:] {
		require.GreaterOrEqual(t, prev.StartTime, s.StartTime, "start time before after")

		if prev.StartTime == s.StartTime {
			require.GreaterOrEqual(t, prev.EndTime, s.EndTime, "end time is after")
		}
	}
}
