package object

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
)

// ID is an identifier of a repository object. Repository objects can be stored.
//
//  1. In a single content block, this is the most common case for small objects.
//  2. In a series of content blocks with an indirect block pointing at them (multiple indirections are allowed).
//     This is used for larger files. Object IDs using indirect blocks start with "I"
//
//nolint:recvcheck
type ID struct {
	cid         content.ID
	indirection byte
	compression bool
}

// MarshalJSON implements JSON serialization of IDs.
func (i ID) MarshalJSON() ([]byte, error) {
	s := i.String()

	//nolint:wrapcheck
	return json.Marshal(s)
}

// UnmarshalJSON implements JSON deserialization of IDs.
func (i *ID) UnmarshalJSON(v []byte) error {
	var s string

	if err := json.Unmarshal(v, &s); err != nil {
		return errors.Wrap(err, "unable to unmarshal object ID")
	}

	tmp, err := ParseID(s)
	if err != nil {
		return errors.Wrap(err, "invalid object ID")
	}

	*i = tmp

	return nil
}

// EmptyID is an empty object ID equivalent to an empty string.
//
//nolint:gochecknoglobals
var EmptyID = ID{}

// HasObjectID exposes the identifier of an object.
type HasObjectID interface {
	ObjectID() ID
}

// String returns string representation of ObjectID that is suitable for displaying in the UI.
func (i ID) String() string {
	var (
		indirectPrefix    string
		compressionPrefix string
	)

	switch i.indirection {
	case 0:
		// no prefix
	case 1:
		indirectPrefix = "I"
	default:
		indirectPrefix = strings.Repeat("I", int(i.indirection))
	}

	if i.compression {
		compressionPrefix = "Z"
	}

	return indirectPrefix + compressionPrefix + i.cid.String()
}

// Append appends string representation of ObjectID that is suitable for displaying in the UI.
func (i ID) Append(out []byte) []byte {
	for range i.indirection {
		out = append(out, 'I')
	}

	if i.compression {
		out = append(out, 'Z')
	}

	return i.cid.Append(out)
}

// IndexObjectID returns the object ID of the underlying index object.
func (i ID) IndexObjectID() (ID, bool) {
	if i.indirection > 0 {
		i2 := i
		i2.indirection--

		return i2, true
	}

	return i, false
}

// ContentID returns the ID of the underlying content.
func (i ID) ContentID() (id content.ID, compressed, ok bool) {
	if i.indirection > 0 {
		return content.EmptyID, false, false
	}

	return i.cid, i.compression, true
}

// IDsFromStrings converts strings to IDs.
func IDsFromStrings(str []string) ([]ID, error) {
	var result []ID

	for _, v := range str {
		id, err := ParseID(v)
		if err != nil {
			return nil, err
		}

		result = append(result, id)
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

// DirectObjectID returns direct object ID based on the provided block ID.
func DirectObjectID(contentID content.ID) ID {
	return ID{cid: contentID}
}

// Compressed returns object ID with 'Z' prefix indicating it's compressed.
func Compressed(objectID ID) ID {
	objectID.compression = true
	return objectID
}

// IndirectObjectID returns indirect object ID based on the underlying index object ID.
func IndirectObjectID(indexObjectID ID) ID {
	indexObjectID.indirection++
	return indexObjectID
}

// ParseID converts the specified string into object ID.
func ParseID(s string) (ID, error) {
	var id ID

	for s != "" && s[0] == 'I' {
		id.indirection++

		s = s[1:]
	}

	if s != "" && s[0] == 'Z' {
		id.compression = true

		s = s[1:]
	}

	if s != "" && s[0] == 'D' {
		// no-op, legacy case
		s = s[1:]
	}

	if id.indirection > 0 && id.compression {
		return id, errors.New("malformed object ID - compression and indirection are mutually exclusive")
	}

	cid, err := index.ParseID(s)
	if err != nil {
		return id, errors.Wrapf(err, "malformed content ID: %q", s)
	}

	id.cid = cid

	return id, nil
}
