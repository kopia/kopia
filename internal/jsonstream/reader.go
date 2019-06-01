package jsonstream

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

// Reader reads a stream of JSON objects.
type Reader struct {
	decoder *json.Decoder
	summary interface{}
}

// Read reads the next JSON objects from the stream, returns io.EOF on the end of stream.
func (r *Reader) Read(v interface{}) error {
	if r.decoder.More() {
		return r.decoder.Decode(v)
	}

	if err := ensureDelimiter(r.decoder, json.Delim(']')); err != nil {
		return invalidStreamFormatError(err)
	}

	tok, err := r.decoder.Token()
	if err != nil {
		return invalidStreamFormatError(err)
	}

	switch tok {
	case json.Delim('}'):
		// end of stream, all good
		return io.EOF

	case "summary":
		s := r.summary
		if s == nil {
			s = map[string]interface{}{}
		}
		if err := r.decoder.Decode(s); err != nil {
			return invalidStreamFormatError(err)
		}
	}

	if err := ensureDelimiter(r.decoder, json.Delim('}')); err != nil {
		return invalidStreamFormatError(err)
	}

	return io.EOF
}

func ensureDelimiter(d *json.Decoder, expected json.Delim) error {
	t, err := d.Token()
	if err != nil {
		return err
	}

	if t != expected {
		return errors.Errorf("expected '%v', got %v", expected.String(), t)
	}

	return nil
}
func ensureStringToken(d *json.Decoder, expected string) error {
	t, err := d.Token()
	if err != nil {
		return err
	}

	if s, ok := t.(string); ok {
		if s == expected {
			return nil
		}
	}

	return errors.Errorf("expected '%v', got '%v'", expected, t)
}

func invalidStreamFormatError(cause error) error {
	return errors.Errorf("invalid stream format: %v", cause)
}

// NewReader returns new Reader on top of a given buffered reader.
// The provided header must match the beginning of a stream.
func NewReader(r io.Reader, header string, summary interface{}) (*Reader, error) {
	dr := Reader{
		decoder: json.NewDecoder(r),
		summary: summary,
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('{')); err != nil {
		return nil, invalidStreamFormatError(err)
	}

	if err := ensureStringToken(dr.decoder, "stream"); err != nil {
		return nil, invalidStreamFormatError(err)
	}

	if err := ensureStringToken(dr.decoder, header); err != nil {
		return nil, invalidStreamFormatError(err)
	}

	if err := ensureStringToken(dr.decoder, "entries"); err != nil {
		return nil, invalidStreamFormatError(err)
	}

	if err := ensureDelimiter(dr.decoder, json.Delim('[')); err != nil {
		return nil, invalidStreamFormatError(err)
	}

	return &dr, nil
}
