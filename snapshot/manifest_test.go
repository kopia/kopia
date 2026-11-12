package snapshot_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

func mustParseObjectID(t *testing.T, s string) object.ID {
	t.Helper()

	oid, err := object.ParseID(s)
	require.NoError(t, err)

	return oid
}

func TestDirEntryJSON(t *testing.T) {
	base := snapshot.DirEntry{
		Name:        "f1",
		Type:        snapshot.EntryTypeFile,
		Permissions: 0o644,
		FileSize:    3,
		ModTime:     fs.UTCTimestampFromTime(time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)),
		UserID:      1000,
		GroupID:     1000,
		ObjectID:    mustParseObjectID(t, "abcd"),
	}

	// Entries without hardlink info must serialize exactly as before the fields
	// were added, so that directory object IDs of existing snapshots are unchanged.
	b, err := json.Marshal(base)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"f1","type":"f","mode":"0644","size":3,"mtime":"2020-01-02T03:04:05Z","uid":1000,"gid":1000,"obj":"abcd"}`, string(b))
	require.NotContains(t, string(b), "dev")
	require.NotContains(t, string(b), "ino")
	require.NotContains(t, string(b), "nlink")
	require.False(t, base.HasHardLinkInfo())

	linked := base
	linked.Dev = 42
	linked.Ino = 1001
	linked.NLink = 2

	require.True(t, linked.HasHardLinkInfo())

	b, err = json.Marshal(linked)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"f1","type":"f","mode":"0644","size":3,"mtime":"2020-01-02T03:04:05Z","uid":1000,"gid":1000,"obj":"abcd","dev":42,"ino":1001,"nlink":2}`, string(b))

	var decoded snapshot.DirEntry

	require.NoError(t, json.Unmarshal(b, &decoded))
	require.Equal(t, linked, decoded)

	// A clone must carry the hardlink identity.
	require.Equal(t, &linked, linked.Clone())
}

func TestDirEntryHasHardLinkInfo(t *testing.T) {
	cases := []struct {
		ino, nlink uint64
		want       bool
	}{
		{0, 0, false},
		{0, 2, false},
		{1, 0, false},
		{1, 1, false},
		{1, 2, true},
	}

	for _, tc := range cases {
		de := snapshot.DirEntry{Ino: tc.ino, NLink: tc.nlink}
		require.Equal(t, tc.want, de.HasHardLinkInfo(), "ino=%d nlink=%d", tc.ino, tc.nlink)
	}
}
