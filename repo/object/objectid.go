package object

import (
	"encoding/hex"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

// ID is an identifier of a repository object. Repository objects can be stored.
//
// 1. In a single content block, this is the most common case for small objects.
// 2. In a series of content blocks with an indirect block pointing at them (multiple indirections are allowed).
//    This is used for larger files. Object IDs using indirect blocks start with "I"
type ID string

// HasObjectID exposes the identifier of an object.
type HasObjectID interface {
	ObjectID() ID
}

// String returns string representation of ObjectID that is suitable for displaying in the UI.
func (i ID) String() string {
	return strings.Replace(string(i), "D", "", -1)
}

// IndexObjectID returns the object ID of the underlying index object.
func (i ID) IndexObjectID() (ID, bool) {
	if strings.HasPrefix(string(i), "I") {
		return i[1:], true
	}

	return "", false
}

// ContentID returns the ID of the underlying content.
func (i ID) ContentID() (id content.ID, compressed, ok bool) {
	if strings.HasPrefix(string(i), "D") {
		return content.ID(i[1:]), false, true
	}

	if strings.HasPrefix(string(i), "I") {
		return "", false, false
	}

	if strings.HasPrefix(string(i), "Z") {
		return content.ID(i[1:]), true, true
	}

	return content.ID(i), false, true
}

// Validate checks the ID format for validity and reports any errors.
func (i ID) Validate() error {
	if indexObjectID, ok := i.IndexObjectID(); ok {
		if err := indexObjectID.Validate(); err != nil {
			return errors.Wrapf(err, "invalid indirect object ID %v", i)
		}

		return nil
	}

	contentID, _, ok := i.ContentID()
	if !ok {
		return errors.Errorf("invalid object ID: %v", i)
	}

	if len(contentID) <= 1 {
		return errors.Errorf("missing content ID")
	}

	// odd length - firstcharacter must be a single character between 'g' and 'z'
	if len(contentID)%2 == 1 {
		if contentID[0] < 'g' || contentID[0] > 'z' {
			return errors.Errorf("invalid content ID prefix: %v", contentID)
		}

		contentID = contentID[1:]
	}

	if _, err := hex.DecodeString(string(contentID)); err != nil {
		return errors.Errorf("invalid contentID suffix, must be base-16 encoded: %v", contentID)
	}

	return nil
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
		result = append(result, string(v))
	}

	return result
}

// DirectObjectID returns direct object ID based on the provided block ID.
func DirectObjectID(contentID content.ID) ID {
	return ID(contentID)
}

// Compressed returns object ID with 'Z' prefix indicating it's compressed.
func Compressed(objectID ID) ID {
	return "Z" + objectID
}

// IndirectObjectID returns indirect object ID based on the underlying index object ID.
func IndirectObjectID(indexObjectID ID) ID {
	return "I" + indexObjectID
}

// ParseID converts the specified string into object ID.
func ParseID(s string) (ID, error) {
	i := ID(s)
	return i, i.Validate()
}
