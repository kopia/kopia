package fs

import (
	"bytes"
	"strings"
	"testing"
)

func TestDirectory(t *testing.T) {
	data := strings.Join(
		[]string{
			`{`,
			`"format":{"version":1},`,
			`"entries":[`,
			`{"name":"config.go","mode":"420","size":"937","modTime":"2016-04-02T02:39:44.123456789Z","owner":"500:100","oid":"C4321"},`,
			`{"name":"constants.go","mode":"420","size":"13","modTime":"2016-04-02T02:36:19Z","owner":"500:100"},`,
			`{"name":"doc.go","mode":"420","size":"112","modTime":"2016-04-02T02:45:54Z","owner":"500:100"},`,
			`{"name":"errors.go","mode":"420","size":"506","modTime":"2016-04-02T02:41:03Z","owner":"500:100"},`,
			`{"name":"subdir","mode":"d:420","modTime":"2016-04-06T02:34:10Z","owner":"500:100","oid":"C1234"}`,
			`]}`,
		}, "")

	d, err := ReadDirectory(strings.NewReader(data), "")
	if err != nil {
		t.Errorf("can't read: %v", err)
		return
	}

	b2 := bytes.NewBuffer(nil)
	dw := newDirectoryWriter(b2)

	for _, e := range d {
		dw.WriteEntry(e)
	}
	dw.Close()

	if !bytes.Equal(b2.Bytes(), []byte(data)) {
		t.Errorf("data does not round trip: %v", string(b2.Bytes()))
	}

	cases := []struct {
		name string
	}{
		{"subdir"},
		{"config.go"},
		{"constants.go"},
		{"doc.go"},
		{"errors.go"},
	}

	for _, c := range cases {
		e := d.FindByName(c.name)
		if e == nil {
			t.Errorf("not found, but expected to be found: %v", c.name)
		} else if e.Name != c.name {
			t.Errorf("incorrect name: %v got %v", c.name, e.Name)
		}
	}

	if e := d.FindByName("nosuchdir"); e != nil {
		t.Errorf("found %v, but expected to be found", e.Name)
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
