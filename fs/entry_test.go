package fs

import (
	"log"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

type testEntry struct {
	n     string
	value int

	Entry
}

func (e testEntry) Name() string {
	return e.n
}

func TestEntriesFindByName(t *testing.T) {
	entries := Entries{
		testEntry{n: "aa", value: 3},
		testEntry{n: "cc", value: 5},
		testEntry{n: "ee", value: 6},
	}

	want := testEntry{n: "aa", value: 3}
	if got := entries.FindByName("aa"); got != want {
		t.Errorf("a")
	}

	if got := entries.FindByName("aa0"); got != nil {
		t.Errorf("unexpected result when looking for non-existent: %v", got)
	}

	if got := entries.FindByName("dd"); got != nil {
		t.Errorf("unexpected result when looking for non-existent: %v", got)
	}

	if got := entries.FindByName("ff"); got != nil {
		t.Errorf("unexpected result when looking for non-existent: %v", got)
	}
}

func TestEntriesSort(t *testing.T) {
	entries := Entries{
		testEntry{n: "cc"},
		testEntry{n: "bb"},
		testEntry{n: "aa"},
	}

	entries.Sort()

	want := Entries{
		testEntry{n: "aa"},
		testEntry{n: "bb"},
		testEntry{n: "cc"},
	}

	if diff := pretty.Compare(entries, want); diff != "" {
		t.Errorf("unexpected output diff (-got, +want): %v", diff)
	}
}

func TestEntriesUpdate(t *testing.T) {
	cases := []struct {
		desc         string
		base         Entries
		updatedEntry Entry
		want         Entries
	}{
		{
			desc: "update existing - first",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			updatedEntry: testEntry{n: "aa", value: 3},
			want: Entries{
				testEntry{n: "aa", value: 3},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
		},
		{
			desc: "update existing in the middle",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			updatedEntry: testEntry{n: "bb", value: 3},
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb", value: 3},
				testEntry{n: "cc"},
			},
		},
		{
			desc: "update existing - last",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			updatedEntry: testEntry{n: "cc", value: 3},
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc", value: 3},
			},
		},
		{
			desc: "insert before first",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			updatedEntry: testEntry{n: "00", value: 3},
			want: Entries{
				testEntry{n: "00", value: 3},
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
		},
		{
			desc: "insert between 2 existing",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "cc"},
				testEntry{n: "dd"},
			},
			updatedEntry: testEntry{n: "bb", value: 3},
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb", value: 3},
				testEntry{n: "cc"},
				testEntry{n: "dd"},
			},
		},
		{
			desc: "insert after last first",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			updatedEntry: testEntry{n: "dd", value: 3},
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
				testEntry{n: "dd", value: 3},
			},
		},
		{
			desc:         "append to empty",
			base:         Entries{},
			updatedEntry: testEntry{n: "dd", value: 3},
			want: Entries{
				testEntry{n: "dd", value: 3},
			},
		},
	}

	for _, tc := range cases {
		log.Printf("starting %q", tc.desc)
		updated := tc.base.Update(tc.updatedEntry)

		if diff := pretty.Compare(updated, tc.want); diff != "" {
			t.Errorf("unexpected output for %q diff (-got, +want): %v", tc.desc, diff)
		}
	}
}

func TestEntriesRemove(t *testing.T) {
	cases := []struct {
		desc         string
		base         Entries
		removedEntry string
		want         Entries
	}{
		{
			desc: "remove existing - first",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			removedEntry: "aa",
			want: Entries{
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
		},
		{
			desc: "remove existing in the middle",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			removedEntry: "bb",
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "cc"},
			},
		},
		{
			desc: "remove existing - last",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			removedEntry: "cc",
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
			},
		},
		{
			desc: "non-existent before first",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			removedEntry: "00",
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
		},
		{
			desc: "non-existent in the middle",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "dd"},
			},
			removedEntry: "cc",
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "dd"},
			},
		},
		{
			desc: "non-existent after last",
			base: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
			removedEntry: "dd",
			want: Entries{
				testEntry{n: "aa"},
				testEntry{n: "bb"},
				testEntry{n: "cc"},
			},
		},
		{
			desc:         "empty",
			base:         Entries{},
			removedEntry: "zz",
			want:         Entries{},
		},
	}

	for _, tc := range cases {
		log.Printf("starting %q", tc.desc)
		updated := tc.base.Remove(tc.removedEntry)

		if diff := pretty.Compare(updated, tc.want); diff != "" {
			t.Errorf("unexpected output for %q diff (-got, +want): %v", tc.desc, diff)
		}
	}
}
