package fs

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
)

var (
	time1 = mustParseTimestamp("2016-04-06T02:34:10Z")
	time2 = mustParseTimestamp("2016-04-02T02:39:44.123456789Z")
	time3 = mustParseTimestamp("2016-04-02T02:36:19Z")
)

func TestFlattenBundles(t *testing.T) {
	base := &repo.ObjectID{StorageBlock: "5555"}
	sources := []*EntryMetadata{
		&EntryMetadata{
			Name:     "bundle1",
			FileSize: 170,
			ObjectId: base,
			BundledChildren: []*EntryMetadata{
				&EntryMetadata{Name: "a1", FileSize: 50},
				&EntryMetadata{Name: "z1", FileSize: 120},
			},
		},
	}

	entries, err := flattenBundles(sources)
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	expectedEntries := []*EntryMetadata{
		&EntryMetadata{Name: "a1", FileSize: 50, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  0,
			Length: 50,
			Base:   base,
		}}},
		&EntryMetadata{Name: "z1", FileSize: 120, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  50,
			Length: 120,
			Base:   base,
		}}},
	}

	verifyDirectory(t, entries, expectedEntries)
}

func TestFlattenBundlesInconsistentBundleSize(t *testing.T) {
	sources := []*EntryMetadata{
		&EntryMetadata{
			Name:     "bundle1",
			FileSize: 171,
			ObjectId: &repo.ObjectID{StorageBlock: "5555"},
			BundledChildren: []*EntryMetadata{
				&EntryMetadata{Name: "a1", FileSize: 50},
				&EntryMetadata{Name: "z1", FileSize: 120},
			},
		},
	}

	_, err := flattenBundles(sources)
	if err == nil {
		t.Errorf("expected error")
		return
	}

	if ok := strings.Contains(err.Error(), "inconsistent size of 'bundle1'"); !ok {
		t.Errorf("invalid error: %v", err)
	}
}

func TestFlattenThreeBundles(t *testing.T) {
	base1 := &repo.ObjectID{StorageBlock: "5555"}
	base2 := &repo.ObjectID{StorageBlock: "6666"}
	base3 := &repo.ObjectID{StorageBlock: "7777"}
	sources := []*EntryMetadata{
		&EntryMetadata{
			Name:     "bundle1",
			FileSize: 170,
			ObjectId: base1,
			BundledChildren: []*EntryMetadata{
				&EntryMetadata{Name: "a1", FileSize: 50},
				&EntryMetadata{Name: "z1", FileSize: 120},
			},
		},
		&EntryMetadata{
			Name:     "bundle3",
			FileSize: 7,
			ObjectId: base3,
			BundledChildren: []*EntryMetadata{
				&EntryMetadata{Name: "a3", FileSize: 5},
				&EntryMetadata{Name: "z3", FileSize: 2},
			},
		},
		&EntryMetadata{
			Name:     "bundle2",
			FileSize: 300,
			ObjectId: base2,
			BundledChildren: []*EntryMetadata{
				&EntryMetadata{Name: "a2", FileSize: 100},
				&EntryMetadata{Name: "z2", FileSize: 200},
			},
		},
	}

	entries, err := flattenBundles(sources)
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	expectedEntries := []*EntryMetadata{
		&EntryMetadata{Name: "a1", FileSize: 50, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  0,
			Length: 50,
			Base:   base1,
		}}},
		&EntryMetadata{Name: "a2", FileSize: 100, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  0,
			Length: 100,
			Base:   base2,
		}}},
		&EntryMetadata{Name: "a3", FileSize: 5, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  0,
			Length: 5,
			Base:   base3,
		}}},
		&EntryMetadata{Name: "z1", FileSize: 120, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  50,
			Length: 120,
			Base:   base1,
		}}},
		&EntryMetadata{Name: "z2", FileSize: 200, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  100,
			Length: 200,
			Base:   base2,
		}}},
		&EntryMetadata{Name: "z3", FileSize: 2, ObjectId: &repo.ObjectID{Section: &repo.ObjectID_Section{
			Start:  5,
			Length: 2,
			Base:   base3,
		}}},
	}

	verifyDirectory(t, entries, expectedEntries)
}

func verifyDirectory(t *testing.T, entries []*EntryMetadata, expectedEntries []*EntryMetadata) {
	if len(entries) != len(expectedEntries) {
		t.Errorf("expected %v entries, got %v", len(expectedEntries), len(entries))
	}

	for i, expected := range expectedEntries {
		if i < len(entries) {
			actual := entries[i]

			if !reflect.DeepEqual(expected, actual) {
				t.Errorf("invalid entry at index %v:\nexpected: %v\nactual:   %v", i,
					expected.String(), actual.String())
			}
		}
	}
}

func TestDirectoryNameOrder(t *testing.T) {
	sortedNames := []string{
		"a/a/a",
		"a/a/",
		"a/b",
		"a/b1",
		"a/b2",
		"a/",
		"bar/a/a",
		"bar/a/",
		"bar/a.b",
		"bar/a.c/",
		"bar/a1/a",
		"bar/a1/",
		"bar/a2",
		"bar/a3",
		"bar/",
		"foo/a/a",
		"foo/a/",
		"foo/b",
		"foo/c/a",
		"foo/c/",
		"foo/d/",
		"foo/e1/",
		"foo/e2/",
		"foo/",
		"goo/a/a",
		"goo/a/",
		"goo/",
	}

	for i, n1 := range sortedNames {
		for j, n2 := range sortedNames {
			expected := i <= j
			actual := isLessOrEqual(n1, n2)
			if actual != expected {
				t.Errorf("unexpected value for isLessOrEqual('%v','%v'), expected: %v, got: %v", n1, n2, expected, actual)
			}
		}
	}
}

func mustParseTimestamp(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic("cannot parse timestamp: " + s)
	}
	return t
}
