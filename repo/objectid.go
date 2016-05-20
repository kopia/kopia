package repo

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	binaryEncoding = base64.RawURLEncoding
)

const (
	objectIDEncryptionInfoSeparator = "."
)

// ObjectIDType describes the type of the chunk.
type ObjectIDType string

// ObjectEncryptionInfo represents encryption info associated with ObjectID.
type ObjectEncryptionInfo string

// NoEncryption indicates that the object is not encrypted.
var NoEncryption = ObjectEncryptionInfo("")

const (
	// ObjectIDTypeText represents text-only inline object ID
	ObjectIDTypeText ObjectIDType = "T"

	// ObjectIDTypeBinary represents binary inline object ID
	ObjectIDTypeBinary ObjectIDType = "B"

	// ObjectIDTypeStored represents ID of object whose data is stored directly in a single storage block indicated by string.
	ObjectIDTypeStored ObjectIDType = "D"

	// ObjectIDTypeList represents ID of an object whose data is stored in mutliple storage blocks.
	// The value of the ObjectID is the list chunk, which lists object IDs that need to be concatenated
	// to form the contents.
	ObjectIDTypeList ObjectIDType = "L" // list chunk
)

// IsStored determines whether data for the given chunk type is stored in the storage
// (as opposed to being stored inline as part of ObjectID itself).
func (ct ObjectIDType) IsStored() bool {
	switch ct {
	case ObjectIDTypeStored, ObjectIDTypeList:
		return true

	default:
		return false
	}
}

// ObjectID represents the identifier of a chunk.
// Identifiers can either refer to data block stored in a Storage, or contain small amounts
// of payload directly (useful for short ASCII or binary files).
type ObjectID string

// Type gets the type of the object ID.
func (c ObjectID) Type() ObjectIDType {
	return ObjectIDType(c[0:1])
}

// InlineData returns inline data stored as part of ObjectID. For chunk IDs representing stored
// chunks, the value is nil.
func (c ObjectID) InlineData() []byte {
	payload := string(c[1:])
	switch c.Type() {
	case ObjectIDTypeText:
		return []byte(payload)

	case ObjectIDTypeBinary:
		decodedPayload, err := binaryEncoding.DecodeString(payload)
		if err == nil {
			return decodedPayload
		}
	}

	return nil
}

// BlockID returns identifier of the storage block. For inline chunk IDs, an empty string is returned.
func (c ObjectID) BlockID() string {
	if c.Type().IsStored() {
		content := string(c[1:])
		firstSeparator := strings.Index(content, objectIDEncryptionInfoSeparator)
		if firstSeparator > 0 {
			return string(content[0:firstSeparator])
		}

		return string(content)
	}

	return ""
}

// EncryptionInfo returns ObjectEncryptionInfo for the ObjectID.
func (c ObjectID) EncryptionInfo() ObjectEncryptionInfo {
	if c.Type().IsStored() {
		content := string(c[1:])
		firstSeparator := strings.Index(content, objectIDEncryptionInfoSeparator)
		if firstSeparator > 0 {
			return ObjectEncryptionInfo(content[firstSeparator+1:])
		}
	}

	return NoEncryption
}

// NewInlineObjectID returns new ObjectID containing inline binary or text-encoded data.
func NewInlineObjectID(data []byte) ObjectID {
	if !utf8.Valid(data) {
		return ObjectID("B" + binaryEncoding.EncodeToString(data))
	}

	for _, b := range data {
		if b < 32 && (b != 9 && b != 10 && b != 13) {
			// Any other character indicates binary content.
			return ObjectID("B" + binaryEncoding.EncodeToString(data))
		}
	}

	return ObjectID("T" + string(data))
}

// ParseObjectID converts the specified string into ObjectID.
func ParseObjectID(objectIDString string) (ObjectID, error) {
	if len(objectIDString) >= 1 {
		chunkType := ObjectIDType(objectIDString[0])
		content := objectIDString[1:]

		switch chunkType {
		case ObjectIDTypeText:
			return ObjectID(objectIDString), nil

		case ObjectIDTypeBinary:
			if _, err := binaryEncoding.DecodeString(content); err == nil {
				return ObjectID(objectIDString), nil
			}

		case ObjectIDTypeList, ObjectIDTypeStored:
			firstSeparator := strings.Index(content, objectIDEncryptionInfoSeparator)
			lastSeparator := strings.LastIndex(content, objectIDEncryptionInfoSeparator)
			if firstSeparator == lastSeparator {
				if firstSeparator == -1 {
					// Found zero Separators in the ID - no encryption info.
					return ObjectID(objectIDString), nil
				}

				if firstSeparator > 0 {
					b, err := hex.DecodeString(content[firstSeparator+1:])
					if err == nil && len(b) > 0 && len(b)%2 == 0 {
						// Valid chunk ID with encryption info.
						return ObjectID(objectIDString), nil
					}
				}
			}
		}
	}

	return "", fmt.Errorf("malformed chunk id: '%s'", objectIDString)
}
