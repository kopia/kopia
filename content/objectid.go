package content

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/kopia/kopia/storage"
)

// ObjectIDType describes the type of the chunk.
type ObjectIDType string

var NoEncryption = ObjectEncryptionInfo("")

type EncryptionMode byte

const (
	ObjectEncryptionNone EncryptionMode = iota
	ObjectEncryptionModeAES256
	objectEncryptionMax
	objectEncryptionInvalid
)

type ObjectEncryptionInfo string

func (oei ObjectEncryptionInfo) Mode() EncryptionMode {
	if len(oei) == 0 {
		return ObjectEncryptionNone
	}

	if len(oei)%2 != 0 {
		return objectEncryptionInvalid
	}

	v, err := strconv.ParseInt(string(oei[0:2]), 16, 8)
	if err != nil {
		return objectEncryptionInvalid
	}

	m := EncryptionMode(v)
	switch m {
	case ObjectEncryptionModeAES256:
		if len(oei) != 66 {
			return objectEncryptionInvalid
		}

	default:
		return objectEncryptionInvalid
	}

	return m
}

const (
	// ObjectIDTypeText represents text-only inline object ID
	ObjectIDTypeText ObjectIDType = "T"

	// ObjectIDTypeBinary represents binary inline object ID
	ObjectIDTypeBinary ObjectIDType = "B"

	// ObjectIDTypeStored represents ID of object whose data is stored directly in a single repository block indicated by BlockID.
	ObjectIDTypeStored ObjectIDType = "C"

	// ObjectIDTypeList represents ID of an object whose data is stored in mutliple repository blocks.
	// The value of the ObjectID is the list chunk, which lists object IDs that need to be concatenated
	// to form the contents.
	ObjectIDTypeList ObjectIDType = "L" // list chunk
)

const (
	// NullObjectID represents empty object ID.
	NullObjectID ObjectID = ""
)

// IsStored determines whether data for the given chunk type is stored in the repository
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
// Identifiers can either refer to data block stored in a Repository, or contain small amounts
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
		decodedPayload, err := base64.StdEncoding.DecodeString(payload)
		if err == nil {
			return decodedPayload
		}
	}

	return nil
}

// BlockID returns identifier of the repository block. For inline chunk IDs, an empty string is returned.
func (c ObjectID) BlockID() storage.BlockID {
	if c.Type().IsStored() {
		content := string(c[1:])
		firstColon := strings.Index(content, ":")
		if firstColon > 0 {
			return storage.BlockID(content[0:firstColon])
		} else {
			return storage.BlockID(content)
		}
	}

	return ""
}

func (c ObjectID) EncryptionInfo() ObjectEncryptionInfo {
	if c.Type().IsStored() {
		content := string(c[1:])
		firstColon := strings.Index(content, ":")
		if firstColon > 0 {
			return ObjectEncryptionInfo(content[firstColon+1:])
		}
	}

	return NoEncryption
}

func NewInlineBinaryObjectID(data []byte) ObjectID {
	return ObjectID("B" + base64.StdEncoding.EncodeToString(data))
}

func NewInlineTextObjectID(text string) ObjectID {
	return ObjectID("T" + text)
}

// ParseObjectID converts the specified string into ObjectID.
func ParseObjectID(objectIDString string) (ObjectID, error) {
	if len(objectIDString) >= 1 {
		chunkType := objectIDString[0:1]
		content := objectIDString[1:]

		switch chunkType {
		case "T":
			return ObjectID(objectIDString), nil

		case "B":
			if _, err := base64.StdEncoding.DecodeString(content); err == nil {
				return ObjectID(objectIDString), nil
			}

		case "C", "L":
			firstColon := strings.Index(content, ":")
			lastColon := strings.LastIndex(content, ":")
			if firstColon == lastColon {
				if firstColon == -1 {
					// Found zero colons in the ID - no encryption info.
					return ObjectID(objectIDString), nil
				}

				if firstColon > 0 {
					b, err := hex.DecodeString(content[firstColon+1:])
					if err == nil && len(b) > 0 {
						// Valid chunk ID with encryption info.
						oid := ObjectID(objectIDString)

						if oid.EncryptionInfo().Mode() < objectEncryptionMax {
							return oid, nil
						}
					}
				}
			}
		}
	}

	return NullObjectID, fmt.Errorf("malformed chunk id: '%s'", objectIDString)
}
