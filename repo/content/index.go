package content

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
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

	GetInfo(contentID ID) (*Info, error)

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

type index struct {
	hdr      headerInfo
	readerAt io.ReaderAt
}

type headerInfo struct {
	keySize    int
	valueSize  int
	entryCount int
}

func readHeader(readerAt io.ReaderAt) (headerInfo, error) {
	var header [8]byte

	if n, err := readerAt.ReadAt(header[:], 0); err != nil || n != 8 {
		return headerInfo{}, errors.Wrap(err, "invalid header")
	}

	if header[0] != 1 {
		return headerInfo{}, errors.Errorf("invalid header format: %v", header[0])
	}

	hi := headerInfo{
		keySize:    int(header[1]),
		valueSize:  int(binary.BigEndian.Uint16(header[2:4])),
		entryCount: int(binary.BigEndian.Uint32(header[4:8])),
	}

	if hi.keySize <= 1 || hi.valueSize < 0 || hi.entryCount < 0 {
		return headerInfo{}, errors.Errorf("invalid header")
	}

	return hi, nil
}

func (b *index) ApproximateCount() int {
	return b.hdr.entryCount
}

// Iterate invokes the provided callback function for a range of contents in the index, sorted alphabetically.
// The iteration ends when the callback returns an error, which is propagated to the caller or when
// all contents have been visited.
func (b *index) Iterate(r IDRange, cb func(Info) error) error {
	startPos, err := b.findEntryPosition(r.StartID)
	if err != nil {
		return errors.Wrap(err, "could not find starting position")
	}

	stride := b.hdr.keySize + b.hdr.valueSize
	entry := make([]byte, stride)

	for i := startPos; i < b.hdr.entryCount; i++ {
		n, err := b.readerAt.ReadAt(entry, int64(packHeaderSize+stride*i))
		if err != nil || n != len(entry) {
			return errors.Wrap(err, "unable to read from index")
		}

		key := entry[0:b.hdr.keySize]
		value := entry[b.hdr.keySize:]

		i, err := b.entryToInfo(bytesToContentID(key), value)
		if err != nil {
			return errors.Wrap(err, "invalid index data")
		}

		if i.ID >= r.EndID {
			break
		}

		if err := cb(i); err != nil {
			return err
		}
	}

	return nil
}

func (b *index) findEntryPosition(contentID ID) (int, error) {
	stride := b.hdr.keySize + b.hdr.valueSize

	var entryArr [maxEntrySize]byte

	var entryBuf []byte

	if stride <= len(entryArr) {
		entryBuf = entryArr[0:stride]
	} else {
		entryBuf = make([]byte, stride)
	}

	var readErr error

	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}
		_, err := b.readerAt.ReadAt(entryBuf, int64(packHeaderSize+stride*p))
		if err != nil {
			readErr = err
			return false
		}

		return bytesToContentID(entryBuf[0:b.hdr.keySize]) >= contentID
	})

	return pos, readErr
}

func (b *index) findEntryPositionExact(idBytes, entryBuf []byte) (int, error) {
	stride := b.hdr.keySize + b.hdr.valueSize

	var readErr error

	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}
		_, err := b.readerAt.ReadAt(entryBuf, int64(packHeaderSize+stride*p))
		if err != nil {
			readErr = err
			return false
		}

		return contentIDBytesGreaterOrEqual(entryBuf[0:b.hdr.keySize], idBytes)
	})

	return pos, readErr
}

func (b *index) findEntry(output []byte, contentID ID) ([]byte, error) {
	var hashBuf [maxContentIDSize]byte

	key := contentIDToBytes(hashBuf[:0], contentID)

	// empty index blob, this is possible when compaction removes exactly everything
	if b.hdr.keySize == unknownKeySize {
		return nil, nil
	}

	if len(key) != b.hdr.keySize {
		return nil, errors.Errorf("invalid content ID: %q (%v vs %v)", contentID, len(key), b.hdr.keySize)
	}

	stride := b.hdr.keySize + b.hdr.valueSize

	var entryArr [maxEntrySize]byte

	var entryBuf []byte

	if stride <= len(entryArr) {
		entryBuf = entryArr[0:stride]
	} else {
		entryBuf = make([]byte, stride)
	}

	position, err := b.findEntryPositionExact(key, entryBuf)
	if err != nil {
		return nil, err
	}

	if position >= b.hdr.entryCount {
		return nil, nil
	}

	if _, err := b.readerAt.ReadAt(entryBuf, int64(packHeaderSize+stride*position)); err != nil {
		return nil, errors.Wrap(err, "error reading header")
	}

	if bytes.Equal(entryBuf[0:len(key)], key) {
		return append(output, entryBuf[len(key):]...), nil
	}

	return nil, nil
}

// GetInfo returns information about a given content. If a content is not found, nil is returned.
func (b *index) GetInfo(contentID ID) (*Info, error) {
	var entryBuf [maxEntrySize]byte

	e, err := b.findEntry(entryBuf[:0], contentID)
	if err != nil {
		return nil, err
	}

	if e == nil {
		return nil, nil
	}

	i, err := b.entryToInfo(contentID, e)
	if err != nil {
		return nil, err
	}

	return &i, err
}

func (b *index) entryToInfo(contentID ID, entryData []byte) (Info, error) {
	var e entry
	if err := e.parse(entryData); err != nil {
		return Info{}, err
	}

	packFile := make([]byte, e.PackFileLength())

	n, err := b.readerAt.ReadAt(packFile, int64(e.PackFileOffset()))
	if err != nil || n != int(e.PackFileLength()) {
		return Info{}, errors.Wrap(err, "can't read pack content ID")
	}

	return Info{
		ID:               contentID,
		Deleted:          e.IsDeleted(),
		TimestampSeconds: e.TimestampSeconds(),
		FormatVersion:    e.PackedFormatVersion(),
		PackOffset:       e.PackedOffset(),
		Length:           e.PackedLength(),
		PackBlobID:       blob.ID(packFile),
	}, nil
}

// Close closes the index and the underlying reader.
func (b *index) Close() error {
	if closer, ok := b.readerAt.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

// openPackIndex reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func openPackIndex(readerAt io.ReaderAt) (packIndex, error) {
	h, err := readHeader(readerAt)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	return &index{hdr: h, readerAt: readerAt}, nil
}
