package repo

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"fmt"
)

// ObjectID represents the identifier of a chunk.
// Identifiers can either refer to data block stored in a Storage, or contain small amounts
// of payload directly (useful for short ASCII or binary files).
type ObjectID struct {
	StorageBlock string           `json:"block,omitempty"`
	Indirect     int32            `json:"indirect,omitempty"`
	Encryption   []byte           `json:"encryption,omitempty"`
	Content      []byte           `json:"content,omitempty"`
	Section      *ObjectIDSection `json:"section,omitempty"`
}

// ObjectIDSection represents details about a section of ObjectID.
type ObjectIDSection struct {
	Start  int64    `json:"start"`
	Length int64    `json:"len"`
	Base   ObjectID `json:"base"`
}

// NullObjectID is the identifier of an null object.
var NullObjectID ObjectID

const objectIDEncryptionInfoSeparator = "."

var (
	inlineContentEncoding = base64.RawURLEncoding
)

// UIString returns the name of the repository object that is suitable for displaying in the UI.
// Note that the object ID name may contain its encryption key, which is sensitive.
func (m *ObjectID) UIString() string {
	if m.StorageBlock != "" {
		var encryptionSuffix string

		if m.Encryption != nil {
			encryptionSuffix = "." + hex.EncodeToString(m.Encryption)
		}

		if m.Indirect > 0 {
			return fmt.Sprintf("L%v,%v%v", m.Indirect, m.StorageBlock, encryptionSuffix)
		}

		return "D" + m.StorageBlock + encryptionSuffix
	}

	if m.Content != nil {
		return "B" + inlineContentEncoding.EncodeToString(m.Content)
	}

	if m.Section != nil {
		return fmt.Sprintf("S%v,%v,%v", m.Section.Start, m.Section.Length, m.Section.Base.UIString())
	}

	return "B"
}

// NewInlineObjectID returns new ObjectID containing inline binary or text-encoded data.
func NewInlineObjectID(data []byte) ObjectID {
	return ObjectID{
		Content: data,
	}
}

// NewSectionObjectID returns new ObjectID representing a section of an object with a given base ID, start offset and length.
func NewSectionObjectID(start, length int64, baseID ObjectID) ObjectID {
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
func ParseObjectID(objectIDString string) (ObjectID, error) {
	if len(objectIDString) >= 1 {
		chunkType := objectIDString[0]
		content := objectIDString[1:]

		switch chunkType {
		case 'S':
			if start, length, base, err := parseSectionInfoString(objectIDString); err == nil {
				return ObjectID{Section: &ObjectIDSection{
					Start:  start,
					Length: length,
					Base:   base,
				}}, nil
			}

		case 'B':
			if v, err := inlineContentEncoding.DecodeString(content); err == nil {
				return ObjectID{Content: v}, nil
			}

		case 'I', 'D':
			var indirectLevel int32
			if chunkType == 'I' {
				comma := strings.Index(content, ",")
				if comma < 0 {
					// malformed
					break
				}
				i, err := strconv.Atoi(content[0:comma])
				if err != nil {
					break
				}
				indirectLevel = int32(i)
				content = content[comma+1:]
			}

			firstSeparator := strings.Index(content, objectIDEncryptionInfoSeparator)
			lastSeparator := strings.LastIndex(content, objectIDEncryptionInfoSeparator)
			if firstSeparator == lastSeparator {
				if firstSeparator == -1 {
					// Found zero Separators in the ID - no encryption info.
					return ObjectID{StorageBlock: content, Indirect: indirectLevel}, nil
				}

				if firstSeparator > 0 {
					b, err := hex.DecodeString(content[firstSeparator+1:])
					if err == nil && len(b) > 0 {
						// Valid chunk ID with encryption info.
						return ObjectID{StorageBlock: content[0:firstSeparator], Encryption: b}, nil
					}
				}
			}
		}
	}

	return NullObjectID, fmt.Errorf("malformed object id: '%s'", objectIDString)
}
