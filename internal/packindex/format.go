package packindex

import (
	"encoding/binary"
	"fmt"
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
	timestampAndFlags uint64 // 48-bit timestamp in seconds since 1970/01/01 UTC, big endian (MSB) + 2 bytes of flags (LSB)
	offset1           uint32 // 4 bytes, big endian
	offset2           uint32 // 4 bytes, big endian
	length1           uint32 // 4 bytes, big endian
}

func (e *entry) parse(b []byte) error {
	if len(b) < 20 {
		return fmt.Errorf("invalid entry length: %v", len(b))
	}

	e.timestampAndFlags = binary.BigEndian.Uint64(b[0:8])
	e.offset1 = binary.BigEndian.Uint32(b[8:12])
	e.offset2 = binary.BigEndian.Uint32(b[12:16])
	e.length1 = binary.BigEndian.Uint32(b[16:20])
	return nil
}

func (e *entry) IsDeleted() bool {
	return e.offset2&0x80000000 != 0
}

func (e *entry) TimestampSeconds() int64 {
	return int64(e.timestampAndFlags >> 16)
}

func (e *entry) PackedFormatVersion() byte {
	return byte(e.timestampAndFlags >> 8)
}

func (e *entry) PackBlockIDLength() byte {
	return byte(e.timestampAndFlags)
}

func (e *entry) PackBlockIDOffset() uint32 {
	return e.offset1
}

func (e *entry) PackedOffset() uint32 {
	return e.offset2 & 0x7fffffff
}

func (e *entry) PackedLength() uint32 {
	return e.length1
}
