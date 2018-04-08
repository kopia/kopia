package object

import (
	"encoding/json"

	"fmt"

	"github.com/kopia/kopia/block"
)

// ID is an identifier of a repository object. Repository objects can be stored:
//
// 1. In a single content block, this is the most common case for small objects.
// 2. In a series of content blocks with an indirect block pointing at them (multiple indirections are allowed). This is used for larger files.
//
type ID struct {
	ContentBlockID block.ContentID
	Indirect       *ID
}

// MarshalJSON emits ObjectID in standard string format.
func (oid *ID) MarshalJSON() ([]byte, error) {
	s := oid.String()
	return json.Marshal(&s)
}

// UnmarshalJSON unmarshals Object ID from a JSON string.
func (oid *ID) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	*oid, err = ParseID(s)
	return err
}

// HasObjectID exposes the identifier of an object.
type HasObjectID interface {
	ObjectID() ID
}

// NullID is the identifier of an null/empty object.
var NullID ID

// String returns string representation of ObjectID that is suitable for displaying in the UI.
//
// Note that the object ID name often contains its encryption key, which is sensitive and can be quite long (~100 characters long).
func (oid ID) String() string {
	if oid.Indirect != nil {
		return fmt.Sprintf("I%v", oid.Indirect)
	}

	if oid.ContentBlockID != "" {
		return "D" + string(oid.ContentBlockID)
	}

	return "B"
}

// Validate validates the ObjectID structure.
func (oid *ID) Validate() error {
	var c int
	if len(oid.ContentBlockID) > 0 {
		c++
	}

	if oid.Indirect != nil {
		c++
		if err := oid.Indirect.Validate(); err != nil {
			return fmt.Errorf("invalid indirect object ID %v: %v", oid, err)
		}
	}

	if c != 1 {
		return fmt.Errorf("inconsistent block content: %+v", oid)
	}

	return nil
}

// ParseID converts the specified string into ObjectID.
// The string format matches the output of String() method.
func ParseID(s string) (ID, error) {
	if len(s) >= 1 {
		chunkType := s[0]
		content := s[1:]

		switch chunkType {
		case 'I', 'D':
			if chunkType == 'I' {
				base, err := ParseID(content)
				if err != nil {
					return NullID, err
				}

				return ID{Indirect: &base}, nil
			}

			return ID{ContentBlockID: block.ContentID(content)}, nil
		}
	}

	return NullID, fmt.Errorf("malformed object id: '%s'", s)
}
