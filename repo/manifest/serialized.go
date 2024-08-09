package manifest

import (
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type manifest struct {
	Entries []*manifestEntry `json:"entries"`
}

type manifestEntry struct {
	ID      ID                `json:"id"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"modified"`
	Deleted bool              `json:"deleted,omitempty"`
	Content json.RawMessage   `json:"data"`
}

const (
	objectOpen  = "{"
	objectClose = "}"
	arrayOpen   = "["
	arrayClose  = "]"
)

var errEOF = errors.New("unexpected end of input")

func expectDelimToken(dec *json.Decoder, expectedToken string) error {
	t, err := dec.Token()
	if errors.Is(err, io.EOF) {
		return errors.WithStack(errEOF)
	} else if err != nil {
		return errors.Wrap(err, "reading JSON token")
	}

	d, ok := t.(json.Delim)
	if !ok {
		return errors.Errorf("unexpected token: (%T) %v", t, t)
	} else if d.String() != expectedToken {
		return errors.Errorf("unexpected token; wanted %s, got %s", expectedToken, d)
	}

	return nil
}

func stringToken(dec *json.Decoder) (string, error) {
	t, err := dec.Token()
	if errors.Is(err, io.EOF) {
		return "", errors.WithStack(errEOF)
	} else if err != nil {
		return "", errors.Wrap(err, "reading JSON token")
	}

	l, ok := t.(string)
	if !ok {
		return "", errors.Errorf("unexpected token (%T) %v; wanted field name", t, t)
	}

	return l, nil
}

func decodeManifestArray(r io.Reader) (manifest, error) {
	var (
		dec = json.NewDecoder(r)
		res = manifest{}
	)

	if err := expectDelimToken(dec, objectOpen); err != nil {
		return res, err
	}

	// Need to manually decode fields here since we can't reuse the stdlib
	// decoder due to memory issues.
	allProcessed, err := parseFields(
		dec,
		func(e *manifestEntry) bool {
			res.Entries = append(res.Entries, e)
			return true
		},
	)
	if err != nil {
		return res, err
	}

	if !allProcessed {
		return res, errors.New("didn't see all entries for serialized manifest")
	}

	// Consumes closing object curly brace after we're done. Don't need to check
	// for EOF because json.Decode only guarantees decoding the next JSON item in
	// the stream so this follows that.
	return res, expectDelimToken(dec, objectClose)
}

// forEachDeserializedEntry deserializes json data from the provided reader and
// calls the provided callback for each *manifestEntry. Note that the callback
// may be called on entries even if an error is later encountered while
// deserializing entries.
func forEachDeserializedEntry(
	r io.Reader,
	callback func(*manifestEntry) bool,
) error {
	dec := json.NewDecoder(r)

	if err := expectDelimToken(dec, objectOpen); err != nil {
		return err
	}

	if allProcessed, err := parseFields(dec, callback); err != nil {
		return err
	} else if allProcessed {
		// We can only check for a closing object brace if we actually traversed the
		// full set objects in the stream. This is more of a sanity check than
		// anything.
		return expectDelimToken(dec, objectClose)
	}

	return nil
}

func parseFields(
	dec *json.Decoder,
	callback func(*manifestEntry) bool,
) (bool, error) {
	var (
		seen bool
		err  error

		// Start with true since in general we can't expect the presence of the
		// "entries" field in the json. This allows us to check for a closing object
		// brace even if the field isn't present.
		allProcessed = true
	)

	for dec.More() {
		var l string

		l, err = stringToken(dec)
		if err != nil {
			return false, err
		}

		// Only have `entries` field right now. Skip other fields.
		if !strings.EqualFold("entries", l) {
			continue
		}

		if seen {
			return false, errors.New("repeated Entries field")
		}

		seen = true

		allProcessed, err = forEachArrayEntry(dec, callback)
		if err != nil {
			return allProcessed, err
		}

		if !allProcessed {
			return allProcessed, nil
		}
	}

	return allProcessed, nil
}

// decodeArray decodes *manifestEntry in a json array and calls the provided
// callback on each one. The callback may still be called for some entries even
// if an error occurs later in the stream. Returns true if all array entries
// were processed.
//
// If the callback returns false then this function stops deserializing the
// array and returns false.
//
// This can be made into a generic function pretty easily if it's needed in
// other places.
func forEachArrayEntry(
	dec *json.Decoder,
	callback func(*manifestEntry) bool,
) (bool, error) {
	// Consume starting bracket.
	if err := expectDelimToken(dec, arrayOpen); err != nil {
		return false, err
	}

	// Read elements.
	for dec.More() {
		var tmp *manifestEntry

		if err := dec.Decode(&tmp); err != nil {
			return false, errors.Wrap(err, "decoding array element")
		}

		if !callback(tmp) {
			return false, nil
		}
	}

	// Consume ending bracket.
	return true, expectDelimToken(dec, arrayClose)
}
