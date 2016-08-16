package internal

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"
)

var testHeader1 = []byte("01234567")
var testHeader2 = []byte("0123456x")

func TestProtoStream(t *testing.T) {
	var buf bytes.Buffer

	data := []TestProto{
		TestProto{Name: "foo"},
		TestProto{Name: "bar"},
		TestProto{Name: "baz"},
	}

	w := NewProtoStreamWriter(&buf, testHeader1)
	for _, d := range data {
		if err := w.Write(&d); err != nil {
			t.Errorf("write error: %v", err)
		}
	}
	r := NewProtoStreamReader(bufio.NewReader(&buf), testHeader1)
	var v TestProto
	for _, d := range data {
		if err := r.Read(&v); err != nil {
			t.Errorf("read error: %v", err)
		}
		if v.Name != d.Name {
			t.Errorf("invalid value: '%v', expected '%v'", v.Name, d.Name)
		}
	}
	err := r.Read(&v)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestEmptyStream(t *testing.T) {
	var buf bytes.Buffer

	NewProtoStreamWriter(&buf, testHeader1)

	r := NewProtoStreamReader(bufio.NewReader(&buf), testHeader2)
	var v TestProto
	err := r.Read(&v)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestInvalidHeader(t *testing.T) {
	var buf bytes.Buffer

	w := NewProtoStreamWriter(&buf, testHeader1)
	if err := w.Write(&TestProto{Name: "foo"}); err != nil {
		t.Errorf("write error: %v", err)
	}

	r := NewProtoStreamReader(bufio.NewReader(&buf), testHeader2)
	var v TestProto
	err := r.Read(&v)
	if err == nil {
		t.Errorf("expected error, got none")
	} else if !strings.Contains(err.Error(), "invalid stream header") {
		t.Errorf("got incorrect error: %v", err)
	}
}
