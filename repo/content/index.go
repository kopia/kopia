package content

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

const (
	maxEntrySize     = 256
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

type headerInfo struct {
	version    int
	keySize    int
	valueSize  int
	entryCount int
}

func readHeader(readerAt io.ReaderAt) (headerInfo, error) {
	var header [8]byte

	if n, err := readerAt.ReadAt(header[:], 0); err != nil || n != 8 {
		return headerInfo{}, errors.Wrap(err, "invalid header")
	}

	hi := headerInfo{
		version:    int(header[0]),
		keySize:    int(header[1]),
		valueSize:  int(binary.BigEndian.Uint16(header[2:4])),
		entryCount: int(binary.BigEndian.Uint32(header[4:8])),
	}

	if hi.keySize <= 1 || hi.valueSize < 0 || hi.entryCount < 0 {
		return headerInfo{}, errors.Errorf("invalid header")
	}

	return hi, nil
}

// openPackIndex reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func openPackIndex(readerAt io.ReaderAt, v1PerContentOverhead uint32) (packIndex, error) {
	h, err := readHeader(readerAt)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	if h.version != 1 {
		return nil, errors.Errorf("invalid header format: %v", h.version)
	}

	return &indexV1{hdr: h, readerAt: readerAt, v1PerContentOverhead: v1PerContentOverhead}, nil
}
