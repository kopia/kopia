package object

import (
	"encoding/json"
	"strconv"
	"strings"

	"fmt"
)

// ID is an identifier of a repository object. Repository objects can be stored:
//
// 1. In a single storage block, this is the most common case for objects up to typically ~20MB.
// Storage blocks are encrypted with key specified in EncryptionKey.
//
// 2. In a series of storage blocks with an indirect block pointing at them (multiple indirections are allowed). This is used for larger files.
//
// 3. Packed into larger objects (packs).
//
// ObjectIDs have standard string representation (returned by String() and accepted as input to ParseObjectID()) suitable for using
// in user interfaces, such as command-line tools:
//
// Examples:
//
//   "D295754edeb35c17911b1fdf853f572fe"                  // storage block
//   "ID2c33acbcba3569f943d9e8aaea7817c5"                 // level-1 indirection block
//   "IID2c33acbcba3569f943d9e8aaea7817c5"                // level-2 indirection block
//   "S30,50,D295754edeb35c17911b1fdf853f572fe"           // section of "D295754edeb35c17911b1fdf853f572fe" between [30,80)
//
//
type ID struct {
	StorageBlock string
	Indirect     *ID
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

	if oid.StorageBlock != "" {
		return "D" + oid.StorageBlock
	}

	return "B"
}

// Validate validates the ObjectID structure.
func (oid *ID) Validate() error {
	var c int
	if len(oid.StorageBlock) > 0 {
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
		case 'P':
			// legacy
			parts := strings.Split(content, "@")
			if len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
				return ID{
					StorageBlock: parts[0],
				}, nil
			}

		case 'I', 'D':
			if chunkType == 'I' {
				if len(content) < 2 || content[1] != ',' {
					base, err := ParseID(content)
					if err != nil {
						return NullID, err
					}

					return ID{Indirect: &base}, nil
				}

				// legacy
				comma := strings.Index(content, ",")
				if comma < 0 {
					// malformed
					break
				}
				indirectLevel, err := strconv.Atoi(content[0:comma])
				if err != nil {
					break
				}
				if indirectLevel <= 0 {
					break
				}
				content = content[comma+1:]
				if content == "" {
					break
				}

				o := &ID{StorageBlock: content}
				for i := 0; i < indirectLevel; i++ {
					o = &ID{Indirect: o}
				}

				return *o, nil
			}

			return ID{StorageBlock: content}, nil
		}
	}

	return NullID, fmt.Errorf("malformed object id: '%s'", s)
}
