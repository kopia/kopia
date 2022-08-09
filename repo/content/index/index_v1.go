package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

const (
	// Version1 identifies version 1 of the index, without content-level compression.
	Version1 = 1

	v1HeaderSize    = 8
	v1DeletedMarker = 0x80000000
	v1MaxEntrySize  = 256 // maximum length of content ID + per-entry data combined
	v1EntryLength   = 20
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
	data      []byte
	contentID ID
	b         *indexV1
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

	nameBuf, err := safeSlice(e.b.data, int64(nameOffset), nameLength)
	if err != nil {
		return invalidBlobID
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

func (e indexEntryInfoV1) GetCompressionHeaderID() compression.HeaderID {
	return 0
}

func (e indexEntryInfoV1) GetEncryptionKeyID() byte {
	return 0
}

var _ Info = indexEntryInfoV1{}

type indexV1 struct {
	hdr    v1HeaderInfo
	data   []byte
	closer func() error

	// v1 index does not explicitly store per-content length so we compute it from packed length and fixed overhead
	// provided by the encryptor.
	v1PerContentOverhead uint32
}

func (b *indexV1) ApproximateCount() int {
	return b.hdr.entryCount
}

// Iterate invokes the provided callback function for a range of contents in the index, sorted alphabetically.
// The iteration ends when the callback returns an error, which is propagated to the caller or when
// all contents have been visited.
func (b *indexV1) Iterate(r IDRange, cb func(Info) error) error {
	startPos, err := b.findEntryPosition(r.StartID)
	if err != nil {
		return errors.Wrap(err, "could not find starting position")
	}

	stride := b.hdr.keySize + b.hdr.valueSize

	for i := startPos; i < b.hdr.entryCount; i++ {
		entry, err := safeSlice(b.data, int64(v1HeaderSize+stride*i), stride)
		if err != nil {
			return errors.Wrap(err, "unable to read from index")
		}

		key := entry[0:b.hdr.keySize]

		contentID := bytesToContentID(key)
		if contentID.comparePrefix(r.EndID) >= 0 {
			break
		}

		i, err := b.entryToInfo(contentID, entry[b.hdr.keySize:])
		if err != nil {
			return errors.Wrap(err, "invalid index data")
		}

		if err := cb(i); err != nil {
			return err
		}
	}

	return nil
}

func (b *indexV1) findEntryPosition(contentID IDPrefix) (int, error) {
	stride := b.hdr.keySize + b.hdr.valueSize

	var readErr error

	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}

		key, err := safeSlice(b.data, int64(v1HeaderSize+stride*p), b.hdr.keySize)
		if err != nil {
			readErr = err
			return false
		}

		return bytesToContentID(key).comparePrefix(contentID) >= 0
	})

	return pos, readErr
}

func (b *indexV1) findEntryPositionExact(idBytes []byte) (int, error) {
	stride := b.hdr.keySize + b.hdr.valueSize

	var readErr error

	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}

		key, err := safeSlice(b.data, int64(v1HeaderSize+stride*p), b.hdr.keySize)
		if err != nil {
			readErr = err
			return false
		}

		return contentIDBytesGreaterOrEqual(key, idBytes)
	})

	return pos, readErr
}

func (b *indexV1) findEntry(output []byte, contentID ID) ([]byte, error) {
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

	position, err := b.findEntryPositionExact(key)
	if err != nil {
		return nil, err
	}

	if position >= b.hdr.entryCount {
		return nil, nil
	}

	entryBuf, err := safeSlice(b.data, int64(v1HeaderSize+stride*position), stride)
	if err != nil {
		return nil, errors.Wrap(err, "error reading header")
	}

	if bytes.Equal(entryBuf[0:len(key)], key) {
		return append(output, entryBuf[len(key):]...), nil
	}

	return nil, nil
}

// GetInfo returns information about a given content. If a content is not found, nil is returned.
func (b *indexV1) GetInfo(contentID ID) (Info, error) {
	var entryBuf [v1MaxEntrySize]byte

	e, err := b.findEntry(entryBuf[:0], contentID)
	if err != nil {
		return nil, err
	}

	if e == nil {
		return nil, nil
	}

	return b.entryToInfo(contentID, e)
}

func (b *indexV1) entryToInfo(contentID ID, entryData []byte) (Info, error) {
	if len(entryData) != v1EntryLength {
		return nil, errors.Errorf("invalid entry length: %v", len(entryData))
	}

	return indexEntryInfoV1{entryData, contentID, b}, nil
}

// Close closes the index.
func (b *indexV1) Close() error {
	if closer := b.closer; closer != nil {
		return errors.Wrap(closer(), "error closing index file")
	}

	return nil
}

type indexBuilderV1 struct {
	packBlobIDOffsets map[blob.ID]uint32
	entryCount        int
	keyLength         int
	entryLength       int
	extraDataOffset   uint32
}

// buildV1 writes the pack index to the provided output.
func (b Builder) buildV1(output io.Writer) error {
	allContents := b.sortedContents()
	b1 := &indexBuilderV1{
		packBlobIDOffsets: map[blob.ID]uint32{},
		keyLength:         -1,
		entryLength:       v1EntryLength,
		entryCount:        len(allContents),
	}

	w := bufio.NewWriter(output)

	// prepare extra data to be appended at the end of an index.
	extraData := b1.prepareExtraData(allContents)

	// write header
	header := make([]byte, v1HeaderSize)
	header[0] = 1 // version
	header[1] = byte(b1.keyLength)
	binary.BigEndian.PutUint16(header[2:4], uint16(b1.entryLength))
	binary.BigEndian.PutUint32(header[4:8], uint32(b1.entryCount))

	if _, err := w.Write(header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	// write all sorted contents.
	entry := make([]byte, b1.entryLength)

	for _, it := range allContents {
		if err := b1.writeEntry(w, it, entry); err != nil {
			return errors.Wrap(err, "unable to write entry")
		}
	}

	if _, err := w.Write(extraData); err != nil {
		return errors.Wrap(err, "error writing extra data")
	}

	return errors.Wrap(w.Flush(), "error flushing index")
}

func (b *indexBuilderV1) prepareExtraData(allContents []Info) []byte {
	var extraData []byte

	var hashBuf [maxContentIDSize]byte

	for i, it := range allContents {
		if i == 0 {
			b.keyLength = len(contentIDToBytes(hashBuf[:0], it.GetContentID()))
		}

		if it.GetPackBlobID() != "" {
			if _, ok := b.packBlobIDOffsets[it.GetPackBlobID()]; !ok {
				b.packBlobIDOffsets[it.GetPackBlobID()] = uint32(len(extraData))
				extraData = append(extraData, []byte(it.GetPackBlobID())...)
			}
		}
	}

	b.extraDataOffset = uint32(v1HeaderSize + b.entryCount*(b.keyLength+b.entryLength))

	return extraData
}

func (b *indexBuilderV1) writeEntry(w io.Writer, it Info, entry []byte) error {
	var hashBuf [maxContentIDSize]byte

	k := contentIDToBytes(hashBuf[:0], it.GetContentID())

	if len(k) != b.keyLength {
		return errors.Errorf("inconsistent key length: %v vs %v", len(k), b.keyLength)
	}

	if it.GetCompressionHeaderID() != 0 {
		return errors.Errorf("compression not supported in index v1")
	}

	if it.GetEncryptionKeyID() != 0 {
		return errors.Errorf("encryption key ID not supported in index v1")
	}

	if err := b.formatEntry(entry, it); err != nil {
		return errors.Wrap(err, "unable to format entry")
	}

	if _, err := w.Write(k); err != nil {
		return errors.Wrap(err, "error writing entry key")
	}

	if _, err := w.Write(entry); err != nil {
		return errors.Wrap(err, "error writing entry")
	}

	return nil
}

func (b *indexBuilderV1) formatEntry(entry []byte, it Info) error {
	entryTimestampAndFlags := entry[0:8]
	entryPackFileOffset := entry[8:12]
	entryPackedOffset := entry[12:16]
	entryPackedLength := entry[16:20]
	timestampAndFlags := uint64(it.GetTimestampSeconds()) << 16 //nolint:gomnd

	packBlobID := it.GetPackBlobID()
	if len(packBlobID) == 0 {
		return errors.Errorf("empty pack content ID for %v", it.GetContentID())
	}

	binary.BigEndian.PutUint32(entryPackFileOffset, b.extraDataOffset+b.packBlobIDOffsets[packBlobID])

	if it.GetDeleted() {
		binary.BigEndian.PutUint32(entryPackedOffset, it.GetPackOffset()|v1DeletedMarker)
	} else {
		binary.BigEndian.PutUint32(entryPackedOffset, it.GetPackOffset())
	}

	binary.BigEndian.PutUint32(entryPackedLength, it.GetPackedLength())
	timestampAndFlags |= uint64(it.GetFormatVersion()) << 8 //nolint:gomnd
	timestampAndFlags |= uint64(len(packBlobID))
	binary.BigEndian.PutUint64(entryTimestampAndFlags, timestampAndFlags)

	return nil
}

type v1HeaderInfo struct {
	version    int
	keySize    int
	valueSize  int
	entryCount int
}

func v1ReadHeader(data []byte) (v1HeaderInfo, error) {
	header, err := safeSlice(data, 0, v1HeaderSize)
	if err != nil {
		return v1HeaderInfo{}, errors.Wrap(err, "invalid header")
	}

	hi := v1HeaderInfo{
		version:    int(header[0]),
		keySize:    int(header[1]),
		valueSize:  int(binary.BigEndian.Uint16(header[2:4])),
		entryCount: int(binary.BigEndian.Uint32(header[4:8])),
	}

	if hi.keySize <= 1 || hi.valueSize < 0 || hi.entryCount < 0 {
		return v1HeaderInfo{}, errors.Errorf("invalid header")
	}

	return hi, nil
}

func openV1PackIndex(hdr v1HeaderInfo, data []byte, closer func() error, overhead uint32) (Index, error) {
	return &indexV1{hdr, data, closer, overhead}, nil
}
