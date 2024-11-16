package index

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/logging"
)

// IDPrefix represents a content ID prefix (empty string or single character between 'g' and 'z').
type IDPrefix string

// ValidateSingle returns an error if a given prefix is invalid.
func (p IDPrefix) ValidateSingle() error {
	switch len(p) {
	case 0:
		return nil
	case 1:
		if p[0] >= 'g' && p[0] <= 'z' {
			return nil
		}
	}

	return errors.New("invalid prefix, must be empty or a single letter between 'g' and 'z'")
}

// ID is an identifier of content in content-addressable storage.
//
//nolint:recvcheck
type ID struct {
	data [hashing.MaxHashSize]byte

	// those 2 could be packed into one byte, but that seems like overkill
	prefix byte
	idLen  byte
}

// MarshalJSON implements JSON serialization.
func (i ID) MarshalJSON() ([]byte, error) {
	s := i.String()

	//nolint:wrapcheck
	return json.Marshal(s)
}

// UnmarshalJSON implements JSON deserialization.
func (i *ID) UnmarshalJSON(v []byte) error {
	var s string

	if err := json.Unmarshal(v, &s); err != nil {
		return errors.Wrap(err, "unable to unmarshal ID")
	}

	tmp, err := ParseID(s)
	if err != nil {
		return errors.Wrap(err, "invalid ID")
	}

	*i = tmp

	return nil
}

// Hash returns the hash part of content ID.
func (i ID) Hash() []byte {
	return i.data[0:i.idLen]
}

// EmptyID represents empty content ID.
var EmptyID = ID{} //nolint:gochecknoglobals

// prefixStrings contains precomputed single-character strings for all valid prefixes 'g'..'z'
//
//nolint:gochecknoglobals
var prefixStrings [256]IDPrefix

func init() {
	for i := 'g'; i <= 'z'; i++ {
		prefixStrings[i] = IDPrefix([]byte{byte(i)})
	}
}

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

// AppendToLogBuffer appends content ID to log buffer.
func (i ID) AppendToLogBuffer(sb *logging.Buffer) {
	var buf [128]byte

	if i.prefix != 0 {
		sb.AppendByte(i.prefix)
	}

	hex.Encode(buf[0:i.idLen*2], i.data[0:i.idLen])
	sb.AppendBytes(buf[0 : i.idLen*2])
}

// Append appends content ID to the slice.
func (i ID) Append(out []byte) []byte {
	var buf [128]byte

	if i.prefix != 0 {
		out = append(out, i.prefix)
	}

	hex.Encode(buf[0:i.idLen*2], i.data[0:i.idLen])

	return append(out, buf[0:i.idLen*2]...)
}

// String returns a string representation of ID.
func (i ID) String() string {
	return string(i.Prefix()) + hex.EncodeToString(i.data[:i.idLen])
}

// Prefix returns a one-character prefix of a content ID or an empty string.
func (i ID) Prefix() IDPrefix {
	return prefixStrings[i.prefix]
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
		return EmptyID, errors.New("hash too long")
	}

	if len(hash) == 0 {
		return EmptyID, errors.New("hash too short")
	}

	if err := prefix.ValidateSingle(); err != nil {
		return EmptyID, errors.Wrap(err, "invalid prefix")
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
			return id, errors.New("invalid content prefix")
		}

		s = s[1:]
	}

	if len(s) > 2*len(id.data) {
		return id, errors.New("hash too long")
	}

	n, err := hex.Decode(id.data[:], []byte(s))
	if err != nil {
		return id, errors.Wrap(err, "invalid content hash")
	}

	if n == 0 {
		return id, errors.Errorf("id too short: %q", s0)
	}

	id.idLen = byte(n)

	return id, nil
}
