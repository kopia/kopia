package object

import (
	"encoding/hex"
	"fmt"
	"strings"
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
	return string(i)
}

// IndexObjectID returns the object ID of the underlying index object.
func (i ID) IndexObjectID() (ID, bool) {
	if strings.HasPrefix(string(i), "I") {
		return i[1:], true
	}

	return "", false
}

// BlockID returns the block ID of the underlying content storage block.
func (i ID) BlockID() (string, bool) {
	if strings.HasPrefix(string(i), "D") {
		return string(i[1:]), true
	}
	if strings.HasPrefix(string(i), "I") {
		return "", false
	}

	return string(i), true
}

// Validate checks the ID format for validity and reports any errors.
func (i ID) Validate() error {
	if indexObjectID, ok := i.IndexObjectID(); ok {
		if err := indexObjectID.Validate(); err != nil {
			return fmt.Errorf("invalid indirect object ID %v: %v", i, err)
		}

		return nil
	}

	if blockID, ok := i.BlockID(); ok {
		if len(blockID) < 2 {
			return fmt.Errorf("missing block ID")
		}

		// odd length - firstcharacter must be a single character between 'g' and 'z'
		if len(blockID)%2 == 1 {
			if blockID[0] < 'g' || blockID[0] > 'z' {
				return fmt.Errorf("invalid block ID prefix: %v", blockID)
			}
			blockID = blockID[1:]
		}

		if _, err := hex.DecodeString(blockID); err != nil {
			return fmt.Errorf("invalid blockID suffix, must be base-16 encoded: %v", blockID)
		}

		return nil
	}

	return fmt.Errorf("invalid object ID: %v", i)
}

// DirectObjectID returns direct object ID based on the provided block ID.
func DirectObjectID(blockID string) ID {
	return ID(blockID)
}

// IndirectObjectID returns indirect object ID based on the underlying index object ID.
func IndirectObjectID(indexObjectID ID) ID {
	return "I" + indexObjectID
}

// ParseID converts the specified string into object ID
func ParseID(s string) (ID, error) {
	i := ID(s)
	return i, i.Validate()
}
