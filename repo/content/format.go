package content

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

const (
	timestampShift           = 16
	packedFormatVersionShift = 8
)

// Format describes a format of a single pack index. The actual structure is not used,
// it's purely for documentation purposes.
// The struct is byte-aligned.
type Format struct {
	Version    byte   // format version number must be 0x01
	KeySize    byte   // size of each key in bytes
	EntrySize  uint16 // size of each entry in bytes, big-endian
	EntryCount uint32 // number of sorted (key,value) entries that follow

	Entries []struct {
		Key   []byte // key bytes (KeySize)
		Entry entry
	}

	ExtraData []byte // extra data
}

type entry struct {
	// big endian:
	// 48 most significant bits - 48-bit timestamp in seconds since 1970/01/01 UTC
	// 8 bits - format version (currently == 1)
	// 8 least significant bits - length of pack content ID
	timestampAndFlags uint64 //
	packFileOffset    uint32 // 4 bytes, big endian, offset within index file where pack (blob) ID begins
	packedOffset      uint32 // 4 bytes, big endian, offset within pack file where the contents begin
	packedLength      uint32 // 4 bytes, big endian, content length
}

func (e *entry) parse(b []byte) error {
	if len(b) < entryFixedHeaderLength {
		return errors.Errorf("invalid entry length: %v", len(b))
	}

	e.timestampAndFlags = binary.BigEndian.Uint64(b[0:8])
	e.packFileOffset = binary.BigEndian.Uint32(b[8:12])
	e.packedOffset = binary.BigEndian.Uint32(b[12:16])
	e.packedLength = binary.BigEndian.Uint32(b[16:20])

	return nil
}

func (e *entry) IsDeleted() bool {
	return e.packedOffset&0x80000000 != 0
}

func (e *entry) TimestampSeconds() int64 {
	return int64(e.timestampAndFlags >> timestampShift)
}

func (e *entry) PackedFormatVersion() byte {
	return byte(e.timestampAndFlags >> packedFormatVersionShift)
}

func (e *entry) PackFileLength() byte {
	return byte(e.timestampAndFlags)
}

func (e *entry) PackFileOffset() uint32 {
	return e.packFileOffset
}

func (e *entry) PackedOffset() uint32 {
	return e.packedOffset & math.MaxInt32
}

func (e *entry) PackedLength() uint32 {
	return e.packedLength
}
