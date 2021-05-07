package content

import (
	"io"

	"github.com/pkg/errors"
)

const (
	maxContentIDSize = maxHashSize + 1
	unknownKeySize   = 255
)

// packIndex is a read-only index of packed contents.
type packIndex interface {
	io.Closer

	ApproximateCount() int

	GetInfo(contentID ID) (Info, error)

	// invoked the provided callback for all entries such that entry.ID >= startID and entry.ID < endID
	Iterate(r IDRange, cb func(Info) error) error
}

// IDRange represents a range of IDs.
type IDRange struct {
	StartID ID // inclusive
	EndID   ID // exclusive
}

// Contains determines whether given ID is in the range.
func (r IDRange) Contains(id ID) bool {
	return id >= r.StartID && id < r.EndID
}

const maxIDCharacterPlus1 = "\x7B"

// PrefixRange returns ID range that contains all IDs with a given prefix.
func PrefixRange(prefix ID) IDRange {
	return IDRange{prefix, prefix + maxIDCharacterPlus1}
}

// AllIDs is an IDRange that contains all valid IDs.
var AllIDs = IDRange{"", maxIDCharacterPlus1}

// AllPrefixedIDs is an IDRange that contains all valid IDs prefixed IDs ('g' .. 'z').
var AllPrefixedIDs = IDRange{"g", maxIDCharacterPlus1}

// AllNonPrefixedIDs is an IDRange that contains all valid IDs non-prefixed IDs ('0' .. 'f').
var AllNonPrefixedIDs = IDRange{"0", "g"}

// openPackIndex reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func openPackIndex(readerAt io.ReaderAt, v1PerContentOverhead uint32) (packIndex, error) {
	h, err := v1ReadHeader(readerAt)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	switch h.version {
	case v1IndexVersion:
		return openV1PackIndex(h, readerAt, v1PerContentOverhead)

	case v2IndexVersion:
		return openV2PackIndex(readerAt)

	default:
		return nil, errors.Errorf("invalid header format: %v", h.version)
	}
}
