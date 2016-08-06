package fs

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	time1 = mustParseTimestamp("2016-04-06T02:34:10Z")
	time2 = mustParseTimestamp("2016-04-02T02:39:44.123456789Z")
	time3 = mustParseTimestamp("2016-04-02T02:36:19Z")
)

func TestEmptyDirectory(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[]`,
			`}`,
		}, "")

	expectedEntries := []*EntryMetadata{}

	verifyDirectory(t, data, expectedEntries)
}

func TestDirectoryWithOnlyBundle(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"bundle1","size":"170","perm":"0","oid":"D5555","entries":[`,
			`{"name":"a1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"50"},`,
			`{"name":"z1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"120"}`,
			`]}`,
			`]}`,
		}, "")

	expectedEntries := []*EntryMetadata{
		&EntryMetadata{Name: "a1", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 50, ObjectID: "S0,50,D5555"},
		&EntryMetadata{Name: "z1", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 120, ObjectID: "S50,120,D5555"},
	}

	verifyDirectory(t, data, expectedEntries)
}

func TestInconsistentBundleSize(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"bundle1","size":"170","perm":"0","oid":"D5555","entries":[`,
			`{"name":"a1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"51"},`,
			`{"name":"z1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"120"}`,
			`]}`,
			`]}`,
		}, "")
	verifyDirectoryError(t, data, "inconsistent size of 'bundle1': 170 (got 171)")
}

func TestInvalidBundleHeaderData(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"bundle1","size":"170","perm":"x","oid":"D5555","entries":[`,
			`{"name":"z1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"120"}`,
			`]}`,
			`]}`,
		}, "")
	verifyDirectoryError(t, data, "invalid permissions: 'x'")
}

func TestInvalidBundleEntryData(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"bundle1","size":"170","perm":"0","oid":"D5555","entries":[`,
			`{"perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"51"},`,
			`{"name":"z1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"120"}`,
			`]}`,
			`]}`,
		}, "")
	verifyDirectoryError(t, data, "empty entry name")
}
func TestDirectoryWithoutBundle(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"constants.go","perm":"420","size":"13","mtime":"2016-04-02T02:36:19Z","owner":"500:100","oid":"D5123"}`,
			`]}`,
		}, "")

	expectedEntries := []*EntryMetadata{
		&EntryMetadata{Name: "constants.go", FileMode: 0420, ModTime: time3, OwnerID: 500, GroupID: 100, FileSize: 13, ObjectID: "D5123"},
	}

	verifyDirectory(t, data, expectedEntries)
}

func TestDirectoryWithThreeBundles(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"bundle1","size":"170","perm":"0","oid":"D5555","entries":[`,
			`{"name":"a1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"50"},`,
			`{"name":"z1","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"120"}`,
			`]},`,
			`{"name":"config.go","perm":"420","size":"937","mtime":"2016-04-02T02:39:44.123456789Z","owner":"500:100","oid":"D4321"},`,
			`{"name":"constants.go","perm":"420","size":"13","mtime":"2016-04-02T02:36:19Z","owner":"500:100","oid":"D5123"},`,
			`{"name":"subdir","type":"d","perm":"755","mtime":"2016-04-06T02:34:10Z","owner":"500:100","oid":"D1234"},`,
			`{"name":"bundle3","size":"7","perm":"0","oid":"D8888","entries":[`,
			`{"name":"a3","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"5"},`,
			`{"name":"z3","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"2"}`,
			`]},`,
			`{"name":"bundle2","size":"170","perm":"0","oid":"D6666","entries":[`,
			`{"name":"a2","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"150"},`,
			`{"name":"z2","perm":"500","mtime":"2016-04-06T02:34:10Z","owner":"500:100","size":"20"}`,
			`]}`,
			`]}`,
		}, "")

	expectedEntries := []*EntryMetadata{
		&EntryMetadata{Name: "a1", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 50, ObjectID: "S0,50,D5555"},
		&EntryMetadata{Name: "a2", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 150, ObjectID: "S0,150,D6666"},
		&EntryMetadata{Name: "a3", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 5, ObjectID: "S0,5,D8888"},
		&EntryMetadata{Name: "config.go", FileMode: 0420, ModTime: time2, OwnerID: 500, GroupID: 100, FileSize: 937, ObjectID: "D4321"},
		&EntryMetadata{Name: "constants.go", FileMode: 0420, ModTime: time3, OwnerID: 500, GroupID: 100, FileSize: 13, ObjectID: "D5123"},
		&EntryMetadata{Name: "subdir", FileMode: os.ModeDir | 0755, ModTime: time1, OwnerID: 500, GroupID: 100, ObjectID: "D1234"},
		&EntryMetadata{Name: "z1", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 120, ObjectID: "S50,120,D5555"},
		&EntryMetadata{Name: "z2", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 20, ObjectID: "S150,20,D6666"},
		&EntryMetadata{Name: "z3", FileMode: 0500, ModTime: time1, OwnerID: 500, GroupID: 100, FileSize: 2, ObjectID: "S5,2,D8888"},
	}

	verifyDirectory(t, data, expectedEntries)
}

func verifyDirectory(t *testing.T, data string, expectedEntries []*EntryMetadata) {
	entries, err := readDirectoryMetadataEntries(strings.NewReader(data))
	if err != nil {
		t.Errorf("can't read directory entries: %v", err)
		return
	}

	if len(entries) != len(expectedEntries) {
		t.Errorf("expected %v entries, got %v", len(expectedEntries), len(entries))
	}

	for i, expected := range expectedEntries {
		if i < len(entries) {
			actual := entries[i]

			if !reflect.DeepEqual(expected, actual) {
				t.Errorf("invalid entry at index %v:\nexpected: %#v\nactual:   %#v", i, expected, actual)
			}
		}
	}
}

func TestInvalidJSON(t *testing.T) {
	verifyDirectoryError(t, "{invalid", "invalid character 'i'")
	verifyDirectoryError(t, `{"format":{"version":1},"entries":[{"name":""}]}`, "empty entry name")
	verifyDirectoryError(t, `{"format":{"version":1},"entries":[{"name":"x","perm":"x123"}]}`, "invalid permissions: 'x123'")
	verifyDirectoryError(t, `{"format":{"version":2},"entries":[]}`, "unsupported version: 2")
}

func verifyDirectoryError(t *testing.T, data, expectedError string) {
	entries, err := readDirectoryMetadataEntries(strings.NewReader(data))
	if err == nil {
		t.Errorf("expected error %v, got no error", expectedError)
	} else {
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error containing '%v', got '%v'", expectedError, err.Error())
		}
	}

	if entries != nil {
		t.Errorf("got unexpected result: %#v", entries)
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
