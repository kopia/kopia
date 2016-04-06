package fs

import (
	"bytes"
	"strings"
	"testing"
)

func TestJSONRoundTrip(t *testing.T) {
	data := strings.Join(
		[]string{
			"DIRECTORY:v1",
			`{"name":"subdir","mode":2147484141,"modified":"2016-04-06T02:34:10Z","uid":501,"gid":20,"oid":"C1234"}`,
			`{"name":"config.go","mode":420,"size":"937","modified":"2016-04-02T02:39:44Z","uid":501,"gid":20,"oid":"C4321"}`,
			`{"name":"constants.go","mode":420,"size":"13","modified":"2016-04-02T02:36:19Z","uid":501,"gid":20}`,
			`{"name":"doc.go","mode":420,"size":"112","modified":"2016-04-02T02:45:54Z","uid":501,"gid":20}`,
			`{"name":"errors.go","mode":420,"size":"506","modified":"2016-04-02T02:41:03Z","uid":501,"gid":20}`,
		}, "\n") + "\n"

	d, err := ReadDirectory(strings.NewReader(data))
	if err != nil {
		t.Errorf("can't read: %v", err)
		return
	}
	b2 := bytes.NewBuffer(nil)
	writeDirectoryHeader(b2)
	for e := range d {
		if e.Error != nil {
			t.Errorf("parse error: %v", e.Error)
			continue
		}
		t.Logf("writing %#v", e.Entry)
		writeDirectoryEntry(b2, e.Entry)
	}

	if !bytes.Equal(b2.Bytes(), []byte(data)) {
		t.Errorf("t: %v", string(b2.Bytes()))
	}
}
