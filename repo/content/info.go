package content

import (
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content/index"
)

type (
	// ID is an identifier of content in content-addressable storage.
	ID = index.ID

	// IDPrefix represents a content ID prefix (empty string or single character between 'g' and 'z').
	IDPrefix = index.IDPrefix

	// Info describes a single piece of content.
	Info = index.Info

	// IDRange represents a range of IDs.
	IDRange = index.IDRange
)

// EmptyID is an empty content ID.
//
//nolint:gochecknoglobals
var EmptyID = index.EmptyID

// IDFromHash creates and validates content ID from a prefix and hash.
func IDFromHash(prefix IDPrefix, hash []byte) (ID, error) {
	//nolint:wrapcheck
	return index.IDFromHash(prefix, hash)
}

// ParseID parses the provided string as content ID.
func ParseID(s string) (ID, error) {
	return index.ParseID(s) //nolint:wrapcheck
}

// IDsFromStrings converts strings to IDs.
func IDsFromStrings(str []string) ([]ID, error) {
	var result []ID

	for _, v := range str {
		cid, err := ParseID(v)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid content ID: %q", v)
		}

		result = append(result, cid)
	}

	return result, nil
}

// IDsToStrings converts the IDs to strings.
func IDsToStrings(input []ID) []string {
	var result []string

	for _, v := range input {
		result = append(result, v.String())
	}

	return result
}
