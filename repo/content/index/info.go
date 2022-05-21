package index

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/hashing"
)

// IDPrefix represents a content ID prefix (empty string or single character between 'g' and 'z').
type IDPrefix string

// Validate returns an error if a given prefix is invalid.
func (p IDPrefix) Validate() error {
	switch len(p) {
	case 0:
		return nil
	case 1:
		if p[0] >= 'g' && p[0] <= 'z' {
			return nil
		}
	}

	return errors.Errorf("invalid prefix, must be empty or a single letter between 'g' and 'z'")
}

// ID is an identifier of content in content-addressable storage.
type ID struct {
	data   [hashing.MaxHashSize]byte
	prefix byte
	idLen  byte
}

// MarshalJSON implements JSON serialization.
func (i ID) MarshalJSON() ([]byte, error) {
	s := i.String()

	// nolint:wrapcheck
	return json.Marshal(s)
}

// UnmarshalJSON implements JSON deserialization.
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

// Hash returns the hash part of content ID.
func (i ID) Hash() []byte {
	return i.data[0:i.idLen]
}

// EmptyID represents empty content ID.
var EmptyID = ID{} // nolint:gochecknoglobals

func (i ID) less(other ID) bool {
	if i.prefix != other.prefix {
		if other.prefix == 0 {
			// value is g..z, other is a..f, so i > other
			return false
		}

		return i.prefix < other.prefix
	}

	return bytes.Compare(i.data[:i.idLen], other.data[:other.idLen]) < 0
}

// String returns a string representation of ID.
func (i ID) String() string {
	return string(i.Prefix()) + hex.EncodeToString(i.data[:i.idLen])
}

// Prefix returns a one-character prefix of a content ID or an empty string.
func (i ID) Prefix() IDPrefix {
	if i.prefix == 0 {
		return ""
	}

	return IDPrefix([]byte{i.prefix})
}

// comparePrefix returns the value of strings.Compare(i.String(), p) in an optimized manner.
func (i ID) comparePrefix(p IDPrefix) int {
	switch len(p) {
	case 0:
		// empty ID == "", otherwise greater
		if i == EmptyID {
			return 0
		}

		return 1

	default:
		// slow path
		return strings.Compare(i.String(), string(p))
	}
}

// HasPrefix determines if the given ID has a non-empty prefix.
func (i ID) HasPrefix() bool {
	return i.prefix != 0
}

// IDFromHash constructs content ID from a prefix and a hash.
func IDFromHash(prefix IDPrefix, hash []byte) (ID, error) {
	var id ID

	if len(hash) > len(id.data) {
		return EmptyID, errors.Errorf("id too long")
	}

	if len(prefix) > 0 {
		id.prefix = prefix[0]
	}

	id.idLen = byte(len(hash))
	copy(id.data[:], hash)

	return id, nil
}

// ParseID parses and validates the content ID.
func ParseID(s string) (ID, error) {
	s0 := s

	if s == "" {
		return ID{}, nil
	}

	var id ID

	if len(s)%2 == 1 {
		id.prefix = s[0]

		if id.prefix < 'g' || id.prefix > 'z' {
			return id, errors.Errorf("invalid content prefix")
		}

		s = s[1:]
	}

	n, err := hex.Decode(id.data[:], []byte(s))
	if err != nil {
		return id, errors.Wrap(err, "invalid content hash")
	}

	if n == 0 {
		return id, errors.Errorf("id too short: %q", s0)
	}

	if n > len(id.data) {
		return id, errors.Errorf("id too long: %q", s0)
	}

	id.idLen = byte(n)

	return id, nil
}

// Info is an information about a single piece of content managed by Manager.
type Info interface {
	GetContentID() ID
	GetPackBlobID() blob.ID
	GetTimestampSeconds() int64
	Timestamp() time.Time
	GetOriginalLength() uint32
	GetPackedLength() uint32
	GetPackOffset() uint32
	GetDeleted() bool
	GetFormatVersion() byte
	GetCompressionHeaderID() compression.HeaderID
	GetEncryptionKeyID() byte
}

// InfoStruct is an implementation of Info based on a structure.
type InfoStruct struct {
	ContentID           ID                   `json:"contentID"`
	PackBlobID          blob.ID              `json:"packFile,omitempty"`
	TimestampSeconds    int64                `json:"time"`
	OriginalLength      uint32               `json:"originalLength"`
	PackedLength        uint32               `json:"length"`
	PackOffset          uint32               `json:"packOffset,omitempty"`
	Deleted             bool                 `json:"deleted"`
	FormatVersion       byte                 `json:"formatVersion"`
	CompressionHeaderID compression.HeaderID `json:"compression,omitempty"`
	EncryptionKeyID     byte                 `json:"encryptionKeyID,omitempty"`
}

// GetContentID implements the Info interface.
func (i *InfoStruct) GetContentID() ID { return i.ContentID }

// GetPackBlobID implements the Info interface.
func (i *InfoStruct) GetPackBlobID() blob.ID { return i.PackBlobID }

// GetTimestampSeconds implements the Info interface.
func (i *InfoStruct) GetTimestampSeconds() int64 { return i.TimestampSeconds }

// GetOriginalLength implements the Info interface.
func (i *InfoStruct) GetOriginalLength() uint32 { return i.OriginalLength }

// GetPackedLength implements the Info interface.
func (i *InfoStruct) GetPackedLength() uint32 { return i.PackedLength }

// GetPackOffset implements the Info interface.
func (i *InfoStruct) GetPackOffset() uint32 { return i.PackOffset }

// GetDeleted implements the Info interface.
func (i *InfoStruct) GetDeleted() bool { return i.Deleted }

// GetFormatVersion implements the Info interface.
func (i *InfoStruct) GetFormatVersion() byte { return i.FormatVersion }

// GetCompressionHeaderID implements the Info interface.
func (i *InfoStruct) GetCompressionHeaderID() compression.HeaderID { return i.CompressionHeaderID }

// GetEncryptionKeyID implements the Info interface.
func (i *InfoStruct) GetEncryptionKeyID() byte { return i.EncryptionKeyID }

// Timestamp implements the Info interface.
func (i *InfoStruct) Timestamp() time.Time {
	return time.Unix(i.GetTimestampSeconds(), 0)
}

// ToInfoStruct converts the provided Info to *InfoStruct.
func ToInfoStruct(i Info) *InfoStruct {
	if is, ok := i.(*InfoStruct); ok {
		return is
	}

	return &InfoStruct{
		ContentID:           i.GetContentID(),
		PackBlobID:          i.GetPackBlobID(),
		TimestampSeconds:    i.GetTimestampSeconds(),
		OriginalLength:      i.GetOriginalLength(),
		PackedLength:        i.GetPackedLength(),
		PackOffset:          i.GetPackOffset(),
		Deleted:             i.GetDeleted(),
		FormatVersion:       i.GetFormatVersion(),
		CompressionHeaderID: i.GetCompressionHeaderID(),
		EncryptionKeyID:     i.GetEncryptionKeyID(),
	}
}
