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

// Open reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func Open(data []byte, closer func() error, v1PerContentOverhead uint32) (Index, error) {
	h, err := v1ReadHeader(data)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	switch h.version {
	case Version1:
		return openV1PackIndex(h, data, closer, v1PerContentOverhead)

	case Version2:
		return openV2PackIndex(data, closer)

	default:
		return nil, errors.Errorf("invalid header format: %v", h.version)
	}
}

func safeSlice(data []byte, offset int64, length int) ([]byte, error) {
	if offset < 0 {
		return nil,
			errors.Errorf("invalid offset")
	}

	if length < 0 {
		return nil, errors.Errorf("invalid length")
	}

	if offset+int64(length) > int64(len(data)) {
		return nil, errors.Errorf("invalid length")
	}

	return data[offset : offset+int64(length)], nil
}

func safeSliceString(data []byte, offset int64, length int) (string, error) {
	if offset < 0 {
		return "", errors.Errorf("invalid offset")
	}

	if length < 0 {
		return "", errors.Errorf("invalid length")
	}

	if offset+int64(length) > int64(len(data)) {
		return "", errors.Errorf("invalid length")
	}

	return string(data[offset : offset+int64(length)]), nil
}
