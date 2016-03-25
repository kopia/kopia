package dir

import (
	"bytes"
	"strings"
	"time"

	"testing"
)

func TestWriter(t *testing.T) {
	b := bytes.NewBuffer(nil)
	w := NewWriter(b)

	w.WriteEntry(&Entry{
		Type:     EntryTypeDirectory,
		Name:     "d1",
		Mode:     0555,
		ModTime:  time.Unix(1458876568, 0),
		ObjectID: "foo",
	})

	w.WriteEntry(&Entry{
		Type:     EntryTypeDirectory,
		Name:     "d2",
		Mode:     0754,
		ModTime:  time.Unix(1451871568, 0),
		ObjectID: "bar",
	})

	w.WriteEntry(&Entry{
		Type:     EntryTypeFile,
		Name:     "f1",
		Mode:     0644,
		ModTime:  time.Unix(1451871368, 0),
		ObjectID: "baz",
		Size:     123456,
	})

	w.WriteEntry(&Entry{
		Type:     EntryTypeFile,
		Name:     "f2",
		Mode:     0644,
		ModTime:  time.Unix(1451871331, 123456789),
		ObjectID: "qoo",
		Size:     12,
	})

	assertLines(
		t,
		string(b.Bytes()),
		"DIRECTORY:v1",
		`{"name":"d1","type":"d","mode":"555","modified":"2016-03-25T03:29:28Z","objectID":"foo"}`,
		`{"name":"d2","type":"d","mode":"754","modified":"2016-01-04T01:39:28Z","objectID":"bar"}`,
		`{"name":"f1","type":"f","size":"123456","mode":"644","modified":"2016-01-04T01:36:08Z","objectID":"baz"}`,
		`{"name":"f2","type":"f","size":"12","mode":"644","modified":"2016-01-04T01:35:31.123456789Z","objectID":"qoo"}`,
	)
}

func assertLines(t *testing.T, text string, expectedLines ...string) {
	expected := strings.Join(expectedLines, "\n") + "\n"
	if text != expected {
		t.Errorf("expected: '%v' got '%v'", expected, text)
	}
}
