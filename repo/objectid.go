package repo

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"fmt"
)

// ObjectID is an identifier of a repository object. Repository objects can be stored:
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
type ObjectID struct {
	StorageBlock string
	Indirect     *ObjectID
	Section      *ObjectIDSection
}

// MarshalJSON emits ObjectID in standard string format.
func (oid *ObjectID) MarshalJSON() ([]byte, error) {
	s := oid.String()
	return json.Marshal(&s)
}

// UnmarshalJSON unmarshals Object ID from a JSON string.
func (oid *ObjectID) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	*oid, err = ParseObjectID(s)
	return err
}

// ObjectIDSection represents details about a section of a repository object.
type ObjectIDSection struct {
	Start  int64    `json:"start"`
	Length int64    `json:"len"`
	Base   ObjectID `json:"base"`
}

// HasObjectID exposes the identifier of an object.
type HasObjectID interface {
	ObjectID() ObjectID
}

// NullObjectID is the identifier of an null/empty object.
var NullObjectID ObjectID

var (
	inlineContentEncoding = base64.RawURLEncoding
)

// String returns string representation of ObjectID that is suitable for displaying in the UI.
//
// Note that the object ID name often contains its encryption key, which is sensitive and can be quite long (~100 characters long).
func (oid ObjectID) String() string {
	if oid.Indirect != nil {
		return fmt.Sprintf("I%v", oid.Indirect)
	}

	if oid.StorageBlock != "" {
		return "D" + oid.StorageBlock
	}

	if oid.Section != nil {
		return fmt.Sprintf("S%v,%v,%v", oid.Section.Start, oid.Section.Length, oid.Section.Base.String())
	}

	return "B"
}

// Validate validates the ObjectID structure.
func (oid *ObjectID) Validate() error {
	var c int
	if len(oid.StorageBlock) > 0 {
		c++
	}

	if oid.Section != nil {
		c++
		if err := oid.Section.Base.Validate(); err != nil {
			return fmt.Errorf("invalid section data in %+v", oid)
		}
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

// SectionObjectID returns new ObjectID representing a section of an object with a given base ID, start offset and length.
func SectionObjectID(start, length int64, baseID ObjectID) ObjectID {
	return ObjectID{
		Section: &ObjectIDSection{
			Base:   baseID,
			Start:  start,
			Length: length,
		},
	}
}

// parseNumberUntilComma parses a string of the form "{x},{remainder}" where x is a 64-bit number and remainder is arbitrary string.
// Returns the number and remainder.
func parseNumberUntilComma(s string) (int64, string, error) {
	comma := strings.IndexByte(s, ',')
	if comma < 0 {
		return 0, "", errors.New("missing comma")
	}

	num, err := strconv.ParseInt(s[0:comma], 10, 64)
	if err != nil {
		return 0, "", err
	}

	return num, s[comma+1:], nil
}

func parseSectionInfoString(s string) (int64, int64, ObjectID, error) {
	var start, length int64
	var err error

	start, s, err = parseNumberUntilComma(s[1:])
	if err != nil {
		return 0, -1, NullObjectID, err
	}

	length, s, err = parseNumberUntilComma(s)
	if err != nil {
		return 0, -1, NullObjectID, err
	}

	oid, err := ParseObjectID(s)
	if err != nil {
		return 0, -1, NullObjectID, err
	}

	return start, length, oid, nil
}

// ParseObjectID converts the specified string into ObjectID.
// The string format matches the output of String() method.
func ParseObjectID(s string) (ObjectID, error) {
	if len(s) >= 1 {
		chunkType := s[0]
		content := s[1:]

		switch chunkType {
		case 'P':
			// legacy
			parts := strings.Split(content, "@")
			if len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
				return ObjectID{
					StorageBlock: parts[0],
				}, nil
			}

		case 'S':
			if start, length, base, err := parseSectionInfoString(s); err == nil {
				return ObjectID{Section: &ObjectIDSection{
					Start:  start,
					Length: length,
					Base:   base,
				}}, nil
			}

		case 'I', 'D':
			if chunkType == 'I' {
				if len(content) < 2 || content[1] != ',' {
					base, err := ParseObjectID(content)
					if err != nil {
						return NullObjectID, err
					}

					return ObjectID{Indirect: &base}, nil
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

				o := &ObjectID{StorageBlock: content}
				for i := 0; i < indirectLevel; i++ {
					o = &ObjectID{Indirect: o}
				}

				return *o, nil
			}

			return ObjectID{StorageBlock: content}, nil
		}
	}

	return NullObjectID, fmt.Errorf("malformed object id: '%s'", s)
}
