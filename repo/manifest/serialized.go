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
	Version int              `json:"version,omitempty"`
}

type manifestEntry struct {
	ID      ID                `json:"id"`
	Labels  map[string]string `json:"labels"`
	ModTime time.Time         `json:"modified"`
	Deleted bool              `json:"deleted,omitempty"`
	Content json.RawMessage   `json:"data,omitempty"`
	// For index versions 1 and above, this is the content ID the manifest data
	// resides in.
	ContentID string `json:"contentID,omitempty"`
}

// inMemManifestEntry is the in-memory version of manifests. It adds a few
// additional fields like a format version.
type inMemManifestEntry struct {
	*manifestEntry
	// formatVersion is the version this manifest was serialized with. Separating
	// this out to be for every in-memory struct but not persisted struct allows
	// us to save some space when serializing content.
	formatVersion int
}

type contentSet struct {
	Contents []*manifestContent `json:"contents"`
	Version  int                `json:"version,omitempty"`
}

type manifestContent struct {
	ID      ID              `json:"id"`
	Content json.RawMessage `json:"data"`
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
	if err := parseFields(dec, &res); err != nil {
		return res, err
	}

	// Consumes closing object curly brace after we're done. Don't need to check
	// for EOF because json.Decode only guarantees decoding the next JSON item in
	// the stream so this follows that.
	return res, expectDelimToken(dec, objectClose)
}

func parseFields(dec *json.Decoder, res *manifest) error {
	seenFields := map[string]struct{}{}

	for dec.More() {
		l, err := stringToken(dec)
		if err != nil {
			return err
		}

		if _, ok := seenFields[l]; ok {
			return errors.Errorf("repeated fieldi %q", l)
		}

		// Only have `entries` field right now. Skip other fields.
		switch {
		case strings.EqualFold("entries", l):
			seenFields[l] = struct{}{}

			if err := decodeArray(dec, &res.Entries); err != nil {
				return err
			}

		case strings.EqualFold("version", l):
			if err := dec.Decode(&res.Version); err != nil {
				return errors.Wrap(err, "decoding manifest serialization version")
			}

		default:
			continue
		}
	}

	return nil
}

// decodeArray decodes an array of *manifestEntry and returns them in output. If
// an error occurs output may contain intermediate state.
//
// This can be made into a generic function pretty easily if it's needed in
// other places.
func decodeArray(dec *json.Decoder, output *[]*manifestEntry) error {
	// Consume starting bracket.
	if err := expectDelimToken(dec, arrayOpen); err != nil {
		return err
	}

	// Read elements.
	for dec.More() {
		var tmp *manifestEntry

		if err := dec.Decode(&tmp); err != nil {
			return errors.Wrap(err, "decoding array element")
		}

		*output = append(*output, tmp)
	}

	// Consume ending bracket.
	return expectDelimToken(dec, arrayClose)
}
