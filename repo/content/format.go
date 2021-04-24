package content

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// FormatV1 describes a format of a single pack index. The actual structure is not used,
// it's purely for documentation purposes.
// The struct is byte-aligned.
type FormatV1 struct {
	Version    byte   // format version number must be 0x01
	KeySize    byte   // size of each key in bytes
	EntrySize  uint16 // size of each entry in bytes, big-endian
	EntryCount uint32 // number of sorted (key,value) entries that follow

	Entries []struct {
		Key   []byte // key bytes (KeySize)
		Entry indexEntryInfoV1
	}

	ExtraData []byte // extra data
}

type indexEntryInfoV1 struct {
	data      string // basically a byte array, but immutable
	contentID ID
	b         *index
}

func (e indexEntryInfoV1) GetContentID() ID {
	return e.contentID
}

// entry bytes 0..5: 48-bit big-endian timestamp in seconds since 1970/01/01 UTC.
func (e indexEntryInfoV1) GetTimestampSeconds() int64 {
	return decodeBigEndianUint48(e.data)
}

// entry byte 6: format version (currently always == 1).
func (e indexEntryInfoV1) GetFormatVersion() byte {
	return e.data[6]
}

// entry byte 7: length of pack content ID
// entry bytes 8..11: 4 bytes, big endian, offset within index file where pack (blob) ID begins.
func (e indexEntryInfoV1) GetPackBlobID() blob.ID {
	nameLength := int(e.data[7])
	nameOffset := decodeBigEndianUint32(e.data[8:])

	var nameBuf [256]byte

	n, err := e.b.readerAt.ReadAt(nameBuf[0:nameLength], int64(nameOffset))
	if err != nil || n != nameLength {
		return "-invalid-blob-id-"
	}

	return blob.ID(nameBuf[0:nameLength])
}

// entry bytes 12..15 - deleted flag (MSBit), 31 lower bits encode pack offset.
func (e indexEntryInfoV1) GetDeleted() bool {
	return e.data[12]&0x80 != 0
}

func (e indexEntryInfoV1) GetPackOffset() uint32 {
	const packOffsetMask = 1<<31 - 1
	return decodeBigEndianUint32(e.data[12:]) & packOffsetMask
}

// bytes 16..19: 4 bytes, big endian, content length.
func (e indexEntryInfoV1) GetPackedLength() uint32 {
	return decodeBigEndianUint32(e.data[16:])
}

func (e indexEntryInfoV1) GetOriginalLength() uint32 {
	return e.GetPackedLength() - e.b.v1PerContentOverhead
}

func (e indexEntryInfoV1) Timestamp() time.Time {
	return time.Unix(e.GetTimestampSeconds(), 0)
}

var _ Info = indexEntryInfoV1{}

func decodeBigEndianUint48(d string) int64 {
	return int64(d[0])<<40 | int64(d[1])<<32 | int64(d[2])<<24 | int64(d[3])<<16 | int64(d[4])<<8 | int64(d[5])
}

func decodeBigEndianUint32(d string) uint32 {
	return uint32(d[0])<<24 | uint32(d[1])<<16 | uint32(d[2])<<8 | uint32(d[3])
}
