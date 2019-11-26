package snapshot_test

import (
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/snapshot"
)

func TestStats(t *testing.T) {
	d := mockfs.NewDirectory()
	f := d.AddFile("foo", []byte{'b', 'a', 'r', 'b', 'a', 'z'}, 0)
	tcs := []struct {
		entry fs.Entry
		want  snapshot.Stats
	}{
		{
			entry: d,
			want: snapshot.Stats{
				ExcludedDirCount: 1,
			},
		},
		{
			entry: f,
			want: snapshot.Stats{
				ExcludedFileCount:     1,
				ExcludedTotalFileSize: f.Size(),
			},
		},
	}

	for _, tc := range tcs {
		got := snapshot.Stats{}
		got.AddExcluded(tc.entry)

		if got != tc.want {
			t.Errorf("Stats do not match, got: %#v, want %#v", got, tc.want)
		}
	}
}
