package index

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

const (
	spillingHeaderSize    = maxIDDataLength + 31
	spillingMaxPackID     = 255 // both index formats store pack name length in a byte
	spillingCRCSize       = 4
	spillingMaxRecordSize = spillingHeaderSize + spillingMaxPackID + spillingCRCSize
)

func validateSpillingInfo(i Info) error {
	if int(i.ContentID.idLen) > maxIDDataLength || i.ContentID.idLen == 0 {
		return errors.New("invalid content ID length in index compaction")
	}

	if p := i.ContentID.prefix; p != 0 && (p < 'g' || p > 'z') {
		return errors.New("invalid content ID prefix in index compaction")
	}

	if len(i.PackBlobID) > spillingMaxPackID {
		return errors.New("pack ID too long in index compaction")
	}

	return nil
}

// Runs store every Info field without the timestamp, format-count, or key-length
// restrictions of final index formats. A per-record checksum and the separately
// held run count detect corruption, truncated records, and missing whole records.
func writeSpillingInfo(w io.Writer, i Info, storage []byte) error {
	data := storage[:0]
	data = append(data, i.ContentID.data[:]...)
	data = append(data, i.ContentID.prefix, i.ContentID.idLen)
	data = binary.LittleEndian.AppendUint64(data, uint64(i.TimestampSeconds)) //nolint:gosec // preserve signed bits
	data = binary.LittleEndian.AppendUint32(data, i.OriginalLength)
	data = binary.LittleEndian.AppendUint32(data, i.PackedLength)
	data = binary.LittleEndian.AppendUint32(data, i.PackOffset)
	data = binary.LittleEndian.AppendUint32(data, uint32(i.CompressionHeaderID))

	deleted := byte(0)
	if i.Deleted {
		deleted = 1
	}

	data = append(data, deleted, i.FormatVersion, i.EncryptionKeyID)
	data = binary.LittleEndian.AppendUint16(data, uint16(len(i.PackBlobID))) //nolint:gosec // validated before insertion
	data = append(data, i.PackBlobID...)

	data = binary.LittleEndian.AppendUint32(data, crc32.ChecksumIEEE(data))
	if n, err := w.Write(data); err != nil {
		return errors.Wrap(err, "writing index compaction record")
	} else if n != len(data) {
		return errors.Wrap(io.ErrShortWrite, "writing index compaction record")
	}

	return nil
}

func readSpillingInfo(r io.Reader, storage []byte) (Info, error) {
	var i Info
	if _, err := io.ReadFull(r, storage[:spillingHeaderSize]); err != nil {
		return i, errors.Wrap(err, "reading index compaction record header")
	}

	packLength := int(binary.LittleEndian.Uint16(storage[spillingHeaderSize-2:]))
	if packLength > spillingMaxPackID {
		return i, errors.New("invalid pack ID length in index compaction record")
	}

	end := spillingHeaderSize + packLength
	if _, err := io.ReadFull(r, storage[spillingHeaderSize:end+spillingCRCSize]); err != nil {
		return i, errors.Wrap(err, "reading index compaction record payload")
	}

	if crc32.ChecksumIEEE(storage[:end]) != binary.LittleEndian.Uint32(storage[end:]) {
		return i, errors.New("index compaction record checksum mismatch")
	}

	copy(i.ContentID.data[:], storage[:maxIDDataLength])
	i.ContentID.prefix = storage[maxIDDataLength]
	i.ContentID.idLen = storage[maxIDDataLength+1]
	data := storage[maxIDDataLength+2:]
	i.TimestampSeconds = int64(binary.LittleEndian.Uint64(data)) //nolint:gosec // recover signed bits
	i.OriginalLength = binary.LittleEndian.Uint32(data[8:])
	i.PackedLength = binary.LittleEndian.Uint32(data[12:])
	i.PackOffset = binary.LittleEndian.Uint32(data[16:])

	i.CompressionHeaderID = compression.HeaderID(binary.LittleEndian.Uint32(data[20:]))
	if data[24] > 1 {
		return i, errors.New("invalid deleted flag in index compaction record")
	}

	i.Deleted = data[24] != 0
	i.FormatVersion = data[25]
	i.EncryptionKeyID = data[26]
	i.PackBlobID = blob.ID(storage[spillingHeaderSize:end])

	return i, validateSpillingInfo(i)
}
