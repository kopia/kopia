package dirstream

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
)

var (
	time1 = mustParseTimestamp("2016-04-06T02:34:10Z")
	time2 = mustParseTimestamp("2016-04-02T02:39:44.123456789Z")
	time3 = mustParseTimestamp("2016-04-02T02:36:19Z")
)

func TestFlattenBundles(t *testing.T) {
	base := repo.ObjectID{StorageBlock: "5555"}
	sources := []*fs.EntryMetadata{
		&fs.EntryMetadata{
			Name:     "bundle1",
			FileSize: 170,
			ObjectID: base,
			BundledChildren: []*fs.EntryMetadata{
				&fs.EntryMetadata{Name: "a1", FileSize: 50},
				&fs.EntryMetadata{Name: "z1", FileSize: 120},
			},
		},
	}

	entries, err := flattenBundles(sources)
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	expectedEntries := []*fs.EntryMetadata{
		&fs.EntryMetadata{Name: "a1", FileSize: 50, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  0,
			Length: 50,
			Base:   base,
		}}},
		&fs.EntryMetadata{Name: "z1", FileSize: 120, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  50,
			Length: 120,
			Base:   base,
		}}},
	}

	verifyDirectory(t, entries, expectedEntries)
}

func TestFlattenBundlesInconsistentBundleSize(t *testing.T) {
	sources := []*fs.EntryMetadata{
		&fs.EntryMetadata{
			Name:     "bundle1",
			FileSize: 171,
			ObjectID: repo.ObjectID{StorageBlock: "5555"},
			BundledChildren: []*fs.EntryMetadata{
				&fs.EntryMetadata{Name: "a1", FileSize: 50},
				&fs.EntryMetadata{Name: "z1", FileSize: 120},
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
	base1 := repo.ObjectID{StorageBlock: "5555"}
	base2 := repo.ObjectID{StorageBlock: "6666"}
	base3 := repo.ObjectID{StorageBlock: "7777"}
	sources := []*fs.EntryMetadata{
		&fs.EntryMetadata{
			Name:     "bundle1",
			FileSize: 170,
			ObjectID: base1,
			BundledChildren: []*fs.EntryMetadata{
				&fs.EntryMetadata{Name: "a1", FileSize: 50},
				&fs.EntryMetadata{Name: "z1", FileSize: 120},
			},
		},
		&fs.EntryMetadata{
			Name:     "bundle3",
			FileSize: 7,
			ObjectID: base3,
			BundledChildren: []*fs.EntryMetadata{
				&fs.EntryMetadata{Name: "a3", FileSize: 5},
				&fs.EntryMetadata{Name: "z3", FileSize: 2},
			},
		},
		&fs.EntryMetadata{
			Name:     "bundle2",
			FileSize: 300,
			ObjectID: base2,
			BundledChildren: []*fs.EntryMetadata{
				&fs.EntryMetadata{Name: "a2", FileSize: 100},
				&fs.EntryMetadata{Name: "z2", FileSize: 200},
			},
		},
	}

	entries, err := flattenBundles(sources)
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	expectedEntries := []*fs.EntryMetadata{
		&fs.EntryMetadata{Name: "a1", FileSize: 50, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  0,
			Length: 50,
			Base:   base1,
		}}},
		&fs.EntryMetadata{Name: "a2", FileSize: 100, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  0,
			Length: 100,
			Base:   base2,
		}}},
		&fs.EntryMetadata{Name: "a3", FileSize: 5, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  0,
			Length: 5,
			Base:   base3,
		}}},
		&fs.EntryMetadata{Name: "z1", FileSize: 120, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  50,
			Length: 120,
			Base:   base1,
		}}},
		&fs.EntryMetadata{Name: "z2", FileSize: 200, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  100,
			Length: 200,
			Base:   base2,
		}}},
		&fs.EntryMetadata{Name: "z3", FileSize: 2, ObjectID: repo.ObjectID{Section: &repo.ObjectIDSection{
			Start:  5,
			Length: 2,
			Base:   base3,
		}}},
	}

	verifyDirectory(t, entries, expectedEntries)
}

func verifyDirectory(t *testing.T, entries []*fs.EntryMetadata, expectedEntries []*fs.EntryMetadata) {
	if len(entries) != len(expectedEntries) {
		t.Errorf("expected %v entries, got %v", len(expectedEntries), len(entries))
	}

	for i, expected := range expectedEntries {
		if i < len(entries) {
			actual := entries[i]

			if !reflect.DeepEqual(expected, actual) {
				t.Errorf("invalid entry at index %v:\nexpected: %#v\nactual:   %#v", i,
					expected, actual)
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
