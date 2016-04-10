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
			`  {"d":"subdir","p":"420","t":"2016-04-06T02:34:10Z","o":"500:100","oid":"C1234"},`,
			`  {"f":"config.go","p":"420","s":"937","t":"2016-04-02T02:39:44.123456789Z","o":"500:100","oid":"C4321"},`,
			`  {"f":"constants.go","p":"420","s":"13","t":"2016-04-02T02:36:19Z","o":"500:100"},`,
			`  {"f":"doc.go","p":"420","s":"112","t":"2016-04-02T02:45:54Z","o":"500:100"},`,
			`  {"f":"errors.go","p":"420","s":"506","t":"2016-04-02T02:41:03Z","o":"500:100"}`,
			`]}`,
		}, "\n") + "\n"

	t.Logf("data: %v", data)

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
