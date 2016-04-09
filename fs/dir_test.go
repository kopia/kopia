package fs

import (
	"bytes"
	"strings"
	"testing"
)

func TestDirectory(t *testing.T) {
	data := strings.Join(
		[]string{
			"DIRECTORY:v1",
			`{"name":"subdir","mode":2147484141,"modified":"2016-04-06T02:34:10Z","uid":501,"gid":20,"oid":"C1234"}`,
			`{"name":"config.go","mode":420,"size":"937","modified":"2016-04-02T02:39:44Z","uid":501,"gid":20,"oid":"C4321"}`,
			`{"name":"constants.go","mode":420,"size":"13","modified":"2016-04-02T02:36:19Z","uid":501,"gid":20}`,
			`{"name":"doc.go","mode":420,"size":"112","modified":"2016-04-02T02:45:54Z","uid":501,"gid":20}`,
			`{"name":"errors.go","mode":420,"size":"506","modified":"2016-04-02T02:41:03Z","uid":501,"gid":20}`,
		}, "\n") + "\n"

	d, err := ReadDirectory(strings.NewReader(data), "")
	if err != nil {
		t.Errorf("can't read: %v", err)
		return
	}
	b2 := bytes.NewBuffer(nil)
	writeDirectoryHeader(b2)
	for _, e := range d {
		writeDirectoryEntry(b2, e)
	}

	if !bytes.Equal(b2.Bytes(), []byte(data)) {
		t.Errorf("data does not round trip: %v", string(b2.Bytes()))
	}

	cases := []struct {
		isDir bool
		name  string
	}{
		{true, "subdir"},
		{false, "config.go"},
		{false, "constants.go"},
		{false, "doc.go"},
		{false, "errors.go"},
	}

	for _, c := range cases {
		e := d.FindByName(c.isDir, c.name)
		if e == nil {
			t.Errorf("not found, but expected to be found: %v/%v", c.name, c.isDir)
		} else if e.Name() != c.name {
			t.Errorf("incorrect name: %v/%v got %v", c.name, c.isDir, e.Name())
		}

		if e := d.FindByName(!c.isDir, c.name); e != nil {
			t.Errorf("found %v, but expected to be found: %v/%v", e.Name(), c.name, c.isDir)
		}
	}

	if e := d.FindByName(true, "nosuchdir"); e != nil {
		t.Errorf("found %v, but expected to be found", e.Name())
	}
	if e := d.FindByName(false, "nosuchfile"); e != nil {
		t.Errorf("found %v, but expected to be found", e.Name())
	}
}
