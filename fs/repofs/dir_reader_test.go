package repofs

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

func bundledFileEntry(n string, l int64) *dirEntry {
	return &dirEntry{
		EntryMetadata: fs.EntryMetadata{
			Name:     n,
			FileSize: l,
			Type:     fs.EntryTypeFile,
		},
	}
}

func entryWithSection(n string, l int64, start int64, length int64, baseID repo.ObjectID) *dirEntry {
	return &dirEntry{
		EntryMetadata: fs.EntryMetadata{
			Name:     n,
			FileSize: l,
			Type:     fs.EntryTypeFile,
		},
		ObjectID: repo.SectionObjectID(start, length, baseID),
	}
}

func bundleEntry(n string, l int64, oid repo.ObjectID, children []*dirEntry) *dirEntry {
	return &dirEntry{
		EntryMetadata: fs.EntryMetadata{
			Name:     n,
			FileSize: l,
			Type:     entryTypeBundle,
		},
		ObjectID:        oid,
		BundledChildren: children,
	}
}

func TestFlattenBundles(t *testing.T) {
	base := repo.ObjectID{StorageBlock: "5555"}
	sources := []*dirEntry{
		bundleEntry("bundle1", 170, base, []*dirEntry{
			bundledFileEntry("a1", 50),
			bundledFileEntry("z1", 120),
		}),
	}

	entries, err := flattenBundles(sources)
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	expectedEntries := []*dirEntry{
		entryWithSection("a1", 50, 0, 50, base),
		entryWithSection("z1", 120, 50, 120, base),
	}

	verifyDirectory(t, entries, expectedEntries)
}

func TestFlattenBundlesInconsistentBundleSize(t *testing.T) {
	sources := []*dirEntry{
		bundleEntry("bundle1", 171, repo.ObjectID{StorageBlock: "5555"}, []*dirEntry{
			bundledFileEntry("a1", 50),
			bundledFileEntry("z1", 120),
		}),
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
	sources := []*dirEntry{
		bundleEntry("bundle1", 170, base1, []*dirEntry{
			bundledFileEntry("a1", 50),
			bundledFileEntry("z1", 120),
		}),
		bundleEntry("bundle3", 7, base3, []*dirEntry{
			bundledFileEntry("a3", 5),
			bundledFileEntry("z3", 2),
		}),
		bundleEntry("bundle2", 300, base2, []*dirEntry{
			bundledFileEntry("a2", 100),
			bundledFileEntry("z2", 200),
		}),
	}

	entries, err := flattenBundles(sources)
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	expectedEntries := []*dirEntry{
		entryWithSection("a1", 50, 0, 50, base1),
		entryWithSection("a2", 100, 0, 100, base2),
		entryWithSection("a3", 5, 0, 5, base3),
		entryWithSection("z1", 120, 50, 120, base1),
		entryWithSection("z2", 200, 100, 200, base2),
		entryWithSection("z3", 2, 5, 2, base3),
	}

	verifyDirectory(t, entries, expectedEntries)
}

func verifyDirectory(t *testing.T, entries []*dirEntry, expectedEntries []*dirEntry) {
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
