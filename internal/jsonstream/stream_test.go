package jsonstream

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"strings"
	"testing"
)

type TestObj struct {
	Name string `json:"name,omitempty"`
}

var testHeader1 = "01234567"
var testHeader2 = "0123456x"

func TestStream(t *testing.T) {
	var buf bytes.Buffer

	data := []TestObj{
		TestObj{Name: "foo"},
		TestObj{Name: "bar"},
		TestObj{Name: "baz"},
	}

	w := NewWriter(&buf, testHeader1)
	for _, d := range data {
		if err := w.Write(&d); err != nil {
			t.Errorf("write error: %v", err)
		}
	}
	w.Finalize()
	log.Printf("wrote: %v", string(buf.Bytes()))
	r, err := NewReader(bufio.NewReader(&buf), testHeader1)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	for _, d := range data {
		v := &TestObj{}
		if err := r.Read(v); err != nil {
			t.Errorf("read error: %v", err)
		}
		if v.Name != d.Name {
			t.Errorf("invalid value: '%v', expected '%v'", v.Name, d.Name)
		}
	}
	v := &TestObj{}
	err = r.Read(v)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestInvalidHeader(t *testing.T) {
	var buf bytes.Buffer

	w := NewWriter(&buf, testHeader1)
	if err := w.Write(&TestObj{Name: "foo"}); err != nil {
		t.Errorf("write error: %v", err)
	}

	_, err := NewReader(bufio.NewReader(&buf), testHeader2)
	if err == nil {
		t.Errorf("expected error, got none")
	} else if !strings.Contains(err.Error(), "invalid stream format") {
		t.Errorf("got incorrect error: %v", err)
	}
}
