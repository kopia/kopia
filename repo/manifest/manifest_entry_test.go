package manifest_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/kopia/kopia/repo/manifest"
)

func TestPickLatestID(t *testing.T) {
	t0 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		input []*manifest.EntryMetadata
		want  manifest.ID
	}{
		{
			input: []*manifest.EntryMetadata{},
			want:  "",
		},
		{
			// pick only item
			input: []*manifest.EntryMetadata{
				{ID: "id1", ModTime: t0},
			},
			want: "id1",
		},
		{
			// pick highest time
			input: []*manifest.EntryMetadata{
				{ID: "id1", ModTime: t0},
				{ID: "id2", ModTime: t1},
				{ID: "id3", ModTime: t2},
			},
			want: "id3",
		},
		{
			// pick lexicographically latest ID if all times are the same.
			input: []*manifest.EntryMetadata{
				{ID: "idx", ModTime: t0},
				{ID: "ida", ModTime: t0},
				{ID: "idz", ModTime: t0},
				{ID: "idb", ModTime: t0},
			},
			want: "idz",
		},
	}

	for _, tc := range cases {
		if got := manifest.PickLatestID(tc.input); got != tc.want {
			t.Errorf("invalid result of PickLatestID: %v, want %v", got, tc.want)
		}
	}
}

func TestDedupeEntryMetadataByLabel(t *testing.T) {
	t0 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC)

	theLabel := "the-label"

	manA0 := &manifest.EntryMetadata{ID: "id1", Labels: map[string]string{theLabel: "a"}, ModTime: t0}
	manA1 := &manifest.EntryMetadata{ID: "id2", Labels: map[string]string{theLabel: "a"}, ModTime: t1}
	manA2 := &manifest.EntryMetadata{ID: "id3", Labels: map[string]string{theLabel: "a"}, ModTime: t2}
	manB0 := &manifest.EntryMetadata{ID: "id4", Labels: map[string]string{theLabel: "b"}, ModTime: t0}
	manB1 := &manifest.EntryMetadata{ID: "id5", Labels: map[string]string{theLabel: "b"}, ModTime: t1}
	manC2 := &manifest.EntryMetadata{ID: "id6", Labels: map[string]string{theLabel: "c"}, ModTime: t2}

	cases := []struct {
		input []*manifest.EntryMetadata
		want  []*manifest.EntryMetadata
	}{
		{
			input: []*manifest.EntryMetadata{},
			want:  nil,
		},
		{
			input: []*manifest.EntryMetadata{manA0, manA1, manA2, manB1, manB0, manC2},
			// results will be sorted by time then ID
			want: []*manifest.EntryMetadata{manB1, manA2, manC2},
		},
	}

	for _, tc := range cases {
		got := manifest.DedupeEntryMetadataByLabel(tc.input, theLabel)
		if diff := cmp.Diff(got, tc.want); diff != "" {
			t.Errorf("invalid result of DedupeEntryMetadataByLabel (-got, +want): %v", diff)
		}
	}
}
