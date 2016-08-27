package jsonstream

import (
	"encoding/json"
	"fmt"
	"io"
)

var commaBytes = []byte(",\n")

// Writer writes a stream of JSON objects.
type Writer struct {
	output    io.Writer
	separator []byte
}

// Write JSON object to the output.
func (w *Writer) Write(v interface{}) error {
	if _, err := w.output.Write(w.separator); err != nil {
		return err
	}
	j, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.output.Write(j); err != nil {
		return err
	}
	w.separator = commaBytes

	return nil
}

// Finalize writes the postamble to the JSON stream.
func (w *Writer) Finalize() error {
	fmt.Fprintf(w.output, "\n]}")
	return nil
}

// NewWriter creates a new Writer on top of a specified writer with a specified optional header.
func NewWriter(w io.Writer, header string) *Writer {
	fmt.Fprintf(w, "{\"stream\":\"%v\",\"entries\":[\n", header)
	return &Writer{
		output: w,
	}
}
