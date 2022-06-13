package fs

import (
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
	entries := []Entry{
		testEntry{n: "aa", value: 3},
		testEntry{n: "cc", value: 5},
		testEntry{n: "ee", value: 6},
	}

	want := testEntry{n: "aa", value: 3}
	if got := FindByName(entries, "aa"); got != want {
		t.Errorf("a")
	}

	if got := FindByName(entries, "aa0"); got != nil {
		t.Errorf("unexpected result when looking for non-existent: %v", got)
	}

	if got := FindByName(entries, "dd"); got != nil {
		t.Errorf("unexpected result when looking for non-existent: %v", got)
	}

	if got := FindByName(entries, "ff"); got != nil {
		t.Errorf("unexpected result when looking for non-existent: %v", got)
	}
}

func TestEntriesSort(t *testing.T) {
	entries := []Entry{
		testEntry{n: "cc"},
		testEntry{n: "bb"},
		testEntry{n: "aa"},
	}

	Sort(entries)

	want := []Entry{
		testEntry{n: "aa"},
		testEntry{n: "bb"},
		testEntry{n: "cc"},
	}

	if diff := pretty.Compare(entries, want); diff != "" {
		t.Errorf("unexpected output diff (-got, +want): %v", diff)
	}
}
