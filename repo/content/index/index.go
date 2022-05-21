// Package index manages content indices.
package index

import (
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/hashing"
)

const (
	maxContentIDSize = hashing.MaxHashSize + 1
	unknownKeySize   = 255
)

// Index is a read-only index of packed contents.
type Index interface {
	io.Closer

	ApproximateCount() int
	GetInfo(contentID ID) (Info, error)

	// invoked the provided callback for all entries such that entry.ID >= startID and entry.ID < endID
	Iterate(r IDRange, cb func(Info) error) error
}

// IDRange represents a range of IDs.
type IDRange struct {
	StartID IDPrefix // inclusive
	EndID   IDPrefix // exclusive
}

// Contains determines whether given ID is in the range.
func (r IDRange) Contains(id ID) bool {
	return id.comparePrefix(r.StartID) >= 0 && id.comparePrefix(r.EndID) < 0
}

const maxIDCharacterPlus1 = "\x7B"

// PrefixRange returns ID range that contains all IDs with a given prefix.
func PrefixRange(prefix IDPrefix) IDRange {
	return IDRange{prefix, prefix + maxIDCharacterPlus1}
}

// AllIDs is an IDRange that contains all valid IDs.
// nolint:gochecknoglobals
var AllIDs = IDRange{"", maxIDCharacterPlus1}

// AllPrefixedIDs is an IDRange that contains all valid IDs prefixed IDs ('g' .. 'z').
// nolint:gochecknoglobals
var AllPrefixedIDs = IDRange{"g", maxIDCharacterPlus1}

// AllNonPrefixedIDs is an IDRange that contains all valid IDs non-prefixed IDs ('0' .. 'f').
// nolint:gochecknoglobals
var AllNonPrefixedIDs = IDRange{"0", "g"}

// Open reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func Open(readerAt io.ReaderAt, v1PerContentOverhead uint32) (Index, error) {
	h, err := v1ReadHeader(readerAt)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	switch h.version {
	case Version1:
		return openV1PackIndex(h, readerAt, v1PerContentOverhead)

	case Version2:
		return openV2PackIndex(readerAt)

	default:
		return nil, errors.Errorf("invalid header format: %v", h.version)
	}
}

func readAtAll(ra io.ReaderAt, p []byte, offset int64) error {
	n, err := ra.ReadAt(p, offset)
	if n != len(p) {
		return errors.Errorf("incomplete read at offset %v, got %v bytes, expected %v", offset, n, len(p))
	}

	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}

	return errors.Wrapf(err, "invalid read at offset %v (%v bytes)", offset, len(p))
}
