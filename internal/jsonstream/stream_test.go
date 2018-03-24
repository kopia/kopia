package jsonstream

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog/log"
)

type TestObj struct {
	Name string `json:"name,omitempty"`
}
type TestSummary struct {
	Value int `json:"val"`
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
	log.Printf("wrote: %v", buf.String())
	r, err := NewReader(bufio.NewReader(&buf), testHeader1, nil)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	for _, d := range data {
		v := &TestObj{}
		if readerr := r.Read(v); readerr != nil {
			t.Errorf("read error: %v", readerr)
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

func TestStreamWithSummary(t *testing.T) {
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
	w.FinalizeWithSummary(TestSummary{Value: 123})
	log.Printf("wrote: %v", buf.String())

	var summary TestSummary
	r, err := NewReader(bufio.NewReader(&buf), testHeader1, &summary)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	for _, d := range data {
		v := &TestObj{}
		if readerr := r.Read(v); readerr != nil {
			t.Errorf("read error: %v", readerr)
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
	if got, want := summary.Value, 123; got != want {
		t.Errorf("unexpected summary value: %v, wanted %v", got, want)
	}
}

func TestInvalidHeader(t *testing.T) {
	var buf bytes.Buffer

	w := NewWriter(&buf, testHeader1)
	if err := w.Write(&TestObj{Name: "foo"}); err != nil {
		t.Errorf("write error: %v", err)
	}

	_, err := NewReader(bufio.NewReader(&buf), testHeader2, nil)
	if err == nil {
		t.Errorf("expected error, got none")
	} else if !strings.Contains(err.Error(), "invalid stream format") {
		t.Errorf("got incorrect error: %v", err)
	}
}
