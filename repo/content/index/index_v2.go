package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

const (
	invalidBlobID              = "---invalid---"
	invalidFormatVersion       = 0xFF
	invalidCompressionHeaderID = 0xFFFF
	invalidEncryptionKeyID     = 0xFF
)

const (
	// Version2 identifies version 2 of the index, supporting content-level compression.
	Version2 = 2

	v2IndexHeaderSize       = 17 // size of fixed header at the beginning of index
	v2PackInfoSize          = 5  // size of each pack information blob
	v2MaxFormatCount        = invalidFormatVersion
	v2MaxUniquePackIDCount  = 1 << 24 // max number of packs that can be stored
	v2MaxShortPackIDCount   = 1 << 16 // max number that can be represented using 2 bytes
	v2MaxContentLength      = 1 << 28 // max supported content length (representible using 3.5 bytes)
	v2MaxShortContentLength = 1 << 24 // max content length representible using 3 bytes
	v2MaxPackOffset         = 1 << 30 // max pack offset 1GiB to leave 2 bits for flags
	v2DeletedMarker         = 0x80000000
	v2MaxEntrySize          = 256 // maximum length of content ID + per-entry data combined
)

// layout of v2 index entry:
//    0-3: timestamp bits 0..31 (relative to base time)
//    4-7: pack offset bits 0..29
//         flags:
//            isDeleted                    (1 bit)
//   8-10: original length bits 0..23
//  11-13: packed length bits 0..23
//  14-15: pack ID (lower 16 bits)- index into Packs[]
//
// optional bytes:
//     16: format ID - index into Formats[] - 0 - present if not all formats are identical

//     17: pack ID - bits 16..23 - present if more than 2^16 packs are in a single index

// 18: high-order bits - present if any content length is greater than 2^24 == 16MiB
//
//	original length bits 24..27  (4 hi bits)
//	packed length bits 24..27    (4 lo bits)
const (
	v2EntryOffsetTimestampSeconds      = 0
	v2EntryOffsetPackOffsetAndFlags    = 4
	v2EntryOffsetOriginalLength        = 8
	v2EntryOffsetPackedLength          = 11
	v2EntryOffsetPackBlobID            = 14
	v2EntryMinLength                   = v2EntryOffsetFormatID
	v2EntryOffsetFormatID              = 16 // optional, assumed zero if missing
	v2EntryOffsetFormatIDEnd           = v2EntryOffsetExtendedPackBlobID
	v2EntryOffsetExtendedPackBlobID    = 17 // optional
	v2EntryOffsetExtendedPackBlobIDEnd = v2EntryOffsetHighLengthBits
	v2EntryOffsetHighLengthBits        = 18 // optional
	v2EntryOffsetHighLengthBitsEnd     = v2EntryMaxLength
	v2EntryMaxLength                   = 19

	// flags (at offset v2EntryOffsetPackOffsetAndFlags).
	v2EntryDeletedFlag    = 0x80
	v2EntryPackOffsetMask = 1<<31 - 1

	// high bits (at offset v2EntryOffsetHighLengthBits).
	v2EntryHighLengthShift                   = 24
	v2EntryHighLengthBitsOriginalLengthShift = 4
	v2EntryHghLengthBitsPackedLengthMask     = 0xF

	// PackBlobID bits above 16 are stored in separate byte.
	v2EntryExtendedPackBlobIDShift = 16
)

// layout of v2 format entry
//
//	0-3: compressionID - 32 bit (corresponding to compression.HeaderID)
const (
	v2FormatInfoSize = 6

	v2FormatOffsetCompressionID   = 0
	v2FormatOffsetFormatVersion   = 4
	v2FormatOffsetEncryptionKeyID = 5
)

// FormatV2 describes a format of a single pack index. The actual structure is not used,
// it's purely for documentation purposes.
// The struct is byte-aligned.
type FormatV2 struct {
	Header struct {
		Version           byte   // format version number must be 0x02
		KeySize           byte   // size of each key in bytes
		EntrySize         uint16 // size of each entry in bytes, big-endian
		EntryCount        uint32 // number of sorted (key,value) entries that follow
		EntriesOffset     uint32 // offset where `Entries` begins
		FormatInfosOffset uint32 // offset where `Formats` begins
		NumFormatInfos    uint32
		PacksOffset       uint32 // offset where `Packs` begins
		NumPacks          uint32
		BaseTimestamp     uint32 // base timestamp in unix seconds
	}

	Entries []struct {
		Key   []byte // key bytes (KeySize)
		Entry []byte // entry bytes (EntrySize)
	}

	// each entry contains offset+length of the name of the pack blob, so that each entry can refer to the index
	// and it resolves to a name.
	Packs []struct {
		PackNameLength byte   // length of the filename
		PackNameOffset uint32 // offset to data (within extra data)
	}

	// each entry represents unique content format.
	Formats []indexV2FormatInfo

	ExtraData []byte // extra data
}

type indexV2FormatInfo struct {
	compressionHeaderID compression.HeaderID
	formatVersion       byte
	encryptionKeyID     byte
}

type indexV2 struct {
	hdr         v2HeaderInfo
	data        []byte
	closer      func() error
	formats     []indexV2FormatInfo
	packBlobIDs []blob.ID
}

func (b *indexV2) entryToInfoStruct(contentID ID, data []byte, result *Info) error {
	if len(data) < v2EntryMinLength {
		return errors.Errorf("invalid entry length: %v", len(data))
	}

	result.ContentID = contentID
	result.TimestampSeconds = int64(decodeBigEndianUint32(data[v2EntryOffsetTimestampSeconds:])) + int64(b.hdr.baseTimestamp)
	result.Deleted = data[v2EntryOffsetPackOffsetAndFlags]&v2EntryDeletedFlag != 0
	result.PackOffset = decodeBigEndianUint32(data[v2EntryOffsetPackOffsetAndFlags:]) & v2EntryPackOffsetMask
	result.OriginalLength = decodeBigEndianUint24(data[v2EntryOffsetOriginalLength:])

	if len(data) > v2EntryOffsetHighLengthBits {
		result.OriginalLength |= uint32(data[v2EntryOffsetHighLengthBits]>>v2EntryHighLengthBitsOriginalLengthShift) << v2EntryHighLengthShift
	}

	result.PackedLength = decodeBigEndianUint24(data[v2EntryOffsetPackedLength:])
	if len(data) > v2EntryOffsetHighLengthBits {
		result.PackedLength |= uint32(data[v2EntryOffsetHighLengthBits]&v2EntryHghLengthBitsPackedLengthMask) << v2EntryHighLengthShift
	}

	fid := formatIDIndex(data)
	if fid >= len(b.formats) {
		result.FormatVersion = invalidFormatVersion
		result.CompressionHeaderID = invalidCompressionHeaderID
		result.EncryptionKeyID = invalidEncryptionKeyID
	} else {
		result.FormatVersion = b.formats[fid].formatVersion
		result.CompressionHeaderID = b.formats[fid].compressionHeaderID
		result.EncryptionKeyID = b.formats[fid].encryptionKeyID
	}

	packIDIndex := uint32(decodeBigEndianUint16(data[v2EntryOffsetPackBlobID:]))
	if len(data) > v2EntryOffsetExtendedPackBlobID {
		packIDIndex |= uint32(data[v2EntryOffsetExtendedPackBlobID]) << v2EntryExtendedPackBlobIDShift
	}

	result.PackBlobID = b.getPackBlobIDByIndex(packIDIndex)

	return nil
}

func formatIDIndex(data []byte) int {
	if len(data) > v2EntryOffsetFormatID {
		return int(data[v2EntryOffsetFormatID])
	}

	return 0
}

type v2HeaderInfo struct {
	version       int
	keySize       int
	entrySize     int
	entryCount    int
	packCount     uint
	formatCount   byte
	baseTimestamp uint32 // base timestamp in unix seconds

	// calculated
	entriesOffset int64
	formatsOffset int64
	packsOffset   int64
	entryStride   int64 // guaranteed to be < v2MaxEntrySize
}

func (b *indexV2) getPackBlobIDByIndex(ndx uint32) blob.ID {
	if ndx >= uint32(b.hdr.packCount) { //nolint:gosec
		return invalidBlobID
	}

	return b.packBlobIDs[ndx]
}

func (b *indexV2) ApproximateCount() int {
	return b.hdr.entryCount
}

// Iterate invokes the provided callback function for a range of contents in the index, sorted alphabetically.
// The iteration ends when the callback returns an error, which is propagated to the caller or when
// all contents have been visited.
func (b *indexV2) Iterate(r IDRange, cb func(Info) error) error {
	startPos, err := b.findEntryPosition(r.StartID)
	if err != nil {
		return errors.Wrap(err, "could not find starting position")
	}

	var tmp Info

	for i := startPos; i < b.hdr.entryCount; i++ {
		entry, err := safeSlice(b.data, b.entryOffset(i), int(b.hdr.entryStride))
		if err != nil {
			return errors.Wrap(err, "unable to read from index")
		}

		key := entry[0:b.hdr.keySize]

		contentID := bytesToContentID(key)
		if contentID.comparePrefix(r.EndID) >= 0 {
			break
		}

		if err := b.entryToInfoStruct(contentID, entry[b.hdr.keySize:], &tmp); err != nil {
			return errors.Wrap(err, "invalid index data")
		}

		if err := cb(tmp); err != nil {
			return err
		}
	}

	return nil
}

func (b *indexV2) entryOffset(p int) int64 {
	return b.hdr.entriesOffset + b.hdr.entryStride*int64(p)
}

func (b *indexV2) findEntryPosition(contentID IDPrefix) (int, error) {
	var readErr error

	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}

		entryBuf, err := safeSlice(b.data, b.entryOffset(p), b.hdr.keySize)
		if err != nil {
			readErr = err
			return false
		}

		return bytesToContentID(entryBuf).comparePrefix(contentID) >= 0
	})

	return pos, readErr
}

func (b *indexV2) findEntryPositionExact(idBytes []byte) (int, error) {
	var readErr error

	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}

		entryBuf, err := safeSlice(b.data, b.entryOffset(p), b.hdr.keySize)
		if err != nil {
			readErr = err
			return false
		}

		return contentIDBytesGreaterOrEqual(entryBuf[0:b.hdr.keySize], idBytes)
	})

	return pos, readErr
}

func (b *indexV2) findEntry(contentID ID) ([]byte, error) {
	var hashBuf [maxContentIDSize]byte

	key := contentIDToBytes(hashBuf[:0], contentID)

	// empty index blob, this is possible when compaction removes exactly everything
	if b.hdr.keySize == unknownKeySize {
		return nil, nil
	}

	if len(key) != b.hdr.keySize {
		return nil, errors.Errorf("invalid content ID: %q (%v vs %v)", contentID, len(key), b.hdr.keySize)
	}

	position, err := b.findEntryPositionExact(key)
	if err != nil {
		return nil, err
	}

	if position >= b.hdr.entryCount {
		return nil, nil
	}

	entryBuf, err := safeSlice(b.data, b.entryOffset(position), int(b.hdr.entryStride))
	if err != nil {
		return nil, errors.Wrap(err, "error reading header")
	}

	if bytes.Equal(entryBuf[0:len(key)], key) {
		return entryBuf[len(key):], nil
	}

	return nil, nil
}

// GetInfo returns information about a given content. If a content is not found, nil is returned.
func (b *indexV2) GetInfo(contentID ID, result *Info) (bool, error) {
	e, err := b.findEntry(contentID)
	if err != nil {
		return false, err
	}

	if e == nil {
		return false, nil
	}

	if err := b.entryToInfoStruct(contentID, e, result); err != nil {
		return false, err
	}

	return true, nil
}

// Close closes the index.
func (b *indexV2) Close() error {
	if closer := b.closer; closer != nil {
		return errors.Wrap(closer(), "error closing index file")
	}

	return nil
}

type indexBuilderV2 struct {
	packBlobIDOffsets      map[blob.ID]uint32
	entryCount             int
	keyLength              int
	entrySize              int
	extraDataOffset        uint32
	uniqueFormatInfo2Index map[indexV2FormatInfo]byte
	packID2Index           map[blob.ID]int
	baseTimestamp          int64
}

func indexV2FormatInfoFromInfo(v *Info) indexV2FormatInfo {
	return indexV2FormatInfo{
		formatVersion:       v.FormatVersion,
		compressionHeaderID: v.CompressionHeaderID,
		encryptionKeyID:     v.EncryptionKeyID,
	}
}

// buildUniqueFormatToIndexMap builds a map of unique indexV2FormatInfo to their numeric identifiers.
func buildUniqueFormatToIndexMap(sortedInfos []*Info) map[indexV2FormatInfo]byte {
	result := map[indexV2FormatInfo]byte{}

	for _, v := range sortedInfos {
		key := indexV2FormatInfoFromInfo(v)
		if _, ok := result[key]; !ok {
			result[key] = byte(len(result))
		}
	}

	return result
}

// buildPackIDToIndexMap builds a map of unique blob IDs to their numeric identifiers.
func buildPackIDToIndexMap(sortedInfos []*Info) map[blob.ID]int {
	result := map[blob.ID]int{}

	for _, v := range sortedInfos {
		blobID := v.PackBlobID
		if _, ok := result[blobID]; !ok {
			result[blobID] = len(result)
		}
	}

	return result
}

// maxContentLengths computes max content lengths in the builder.
func maxContentLengths(sortedInfos []*Info) (maxPackedLength, maxOriginalLength, maxPackOffset uint32) {
	for _, v := range sortedInfos {
		if l := v.PackedLength; l > maxPackedLength {
			maxPackedLength = l
		}

		if l := v.OriginalLength; l > maxOriginalLength {
			maxOriginalLength = l
		}

		if l := v.PackOffset; l > maxPackOffset {
			maxPackOffset = l
		}
	}

	return
}

func newIndexBuilderV2(sortedInfos []*Info) (*indexBuilderV2, error) {
	entrySize := v2EntryOffsetFormatID

	// compute a map of unique formats to their indexes.
	uniqueFormat2Index := buildUniqueFormatToIndexMap(sortedInfos)
	if len(uniqueFormat2Index) > v2MaxFormatCount {
		return nil, errors.Errorf("unsupported - too many unique formats %v (max %v)", len(uniqueFormat2Index), v2MaxFormatCount)
	}

	// if have more than one format present, we need to store per-entry format identifier, otherwise assume 0.
	if len(uniqueFormat2Index) > 1 {
		entrySize = max(entrySize, v2EntryOffsetFormatIDEnd)
	}

	packID2Index := buildPackIDToIndexMap(sortedInfos)
	if len(packID2Index) > v2MaxUniquePackIDCount {
		return nil, errors.Errorf("unsupported - too many unique pack IDs %v (max %v)", len(packID2Index), v2MaxUniquePackIDCount)
	}

	if len(packID2Index) > v2MaxShortPackIDCount {
		entrySize = max(entrySize, v2EntryOffsetExtendedPackBlobIDEnd)
	}

	// compute maximum content length to determine how many bits we need to use to store it.
	maxPackedLen, maxOriginalLength, maxPackOffset := maxContentLengths(sortedInfos)

	// contents >= 28 bits (256 MiB) can't be stored at all.
	if maxPackedLen >= v2MaxContentLength || maxOriginalLength >= v2MaxContentLength {
		return nil, errors.Errorf("maximum content length is too high: (packed %v, original %v, max %v)", maxPackedLen, maxOriginalLength, v2MaxContentLength)
	}

	// contents >= 24 bits (16 MiB) requires extra 0.5 byte per length.
	if maxPackedLen >= v2MaxShortContentLength || maxOriginalLength >= v2MaxShortContentLength {
		entrySize = max(entrySize, v2EntryOffsetHighLengthBitsEnd)
	}

	if maxPackOffset >= v2MaxPackOffset {
		return nil, errors.Errorf("pack offset %v is too high", maxPackOffset)
	}

	keyLength := -1

	if len(sortedInfos) > 0 {
		var hashBuf [maxContentIDSize]byte

		keyLength = len(contentIDToBytes(hashBuf[:0], sortedInfos[0].ContentID))
	}

	return &indexBuilderV2{
		packBlobIDOffsets:      map[blob.ID]uint32{},
		keyLength:              keyLength,
		entrySize:              entrySize,
		entryCount:             len(sortedInfos),
		uniqueFormatInfo2Index: uniqueFormat2Index,
		packID2Index:           packID2Index,
	}, nil
}

// buildV2 writes the pack index to the provided output.
func buildV2(sortedInfos []*Info, output io.Writer) error {
	b2, err := newIndexBuilderV2(sortedInfos)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(output)

	// prepare extra data to be appended at the end of an index.
	extraData := b2.prepareExtraData(sortedInfos)

	if b2.keyLength <= 1 {
		return errors.Errorf("invalid key length: %v for %v", b2.keyLength, len(sortedInfos))
	}

	// write header
	header := make([]byte, v2IndexHeaderSize)
	header[0] = Version2 // version
	header[1] = byte(b2.keyLength)
	binary.BigEndian.PutUint16(header[2:4], uint16(b2.entrySize))          //nolint:gosec
	binary.BigEndian.PutUint32(header[4:8], uint32(b2.entryCount))         //nolint:gosec
	binary.BigEndian.PutUint32(header[8:12], uint32(len(b2.packID2Index))) //nolint:gosec
	header[12] = byte(len(b2.uniqueFormatInfo2Index))
	binary.BigEndian.PutUint32(header[13:17], uint32(b2.baseTimestamp)) //nolint:gosec

	if _, err := w.Write(header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	// write sorted index entries
	for _, it := range sortedInfos {
		if err := b2.writeIndexEntry(w, it); err != nil {
			return errors.Wrap(err, "unable to write entry")
		}
	}

	// write pack ID entries in the index order of values from packID2Index (0, 1, 2, ...).
	reversePackIDIndex := make([]blob.ID, len(b2.packID2Index))
	for k, v := range b2.packID2Index {
		reversePackIDIndex[v] = k
	}

	// emit pack ID information in this order.
	for _, e := range reversePackIDIndex {
		if err := b2.writePackIDEntry(w, e); err != nil {
			return errors.Wrap(err, "error writing format info entry")
		}
	}

	// build a list of indexV2FormatInfo using the order of indexes from uniqueFormatInfo2Index.
	reverseFormatInfoIndex := make([]indexV2FormatInfo, len(b2.uniqueFormatInfo2Index))
	for k, v := range b2.uniqueFormatInfo2Index {
		reverseFormatInfoIndex[v] = k
	}

	// emit format information in this order.
	for _, f := range reverseFormatInfoIndex {
		if err := b2.writeFormatInfoEntry(w, f); err != nil {
			return errors.Wrap(err, "error writing format info entry")
		}
	}

	if _, err := w.Write(extraData); err != nil {
		return errors.Wrap(err, "error writing extra data")
	}

	return errors.Wrap(w.Flush(), "error flushing index")
}

func (b *indexBuilderV2) prepareExtraData(sortedInfos []*Info) []byte {
	var extraData []byte

	for _, it := range sortedInfos {
		if it.PackBlobID != "" {
			if _, ok := b.packBlobIDOffsets[it.PackBlobID]; !ok {
				b.packBlobIDOffsets[it.PackBlobID] = uint32(len(extraData)) //nolint:gosec
				extraData = append(extraData, []byte(it.PackBlobID)...)
			}
		}
	}

	b.extraDataOffset = v2IndexHeaderSize // fixed header
	//nolint:gosec
	b.extraDataOffset += uint32(b.entryCount * (b.keyLength + b.entrySize)) // entries index
	//nolint:gosec
	b.extraDataOffset += uint32(len(b.packID2Index) * v2PackInfoSize) // pack information
	//nolint:gosec
	b.extraDataOffset += uint32(len(b.uniqueFormatInfo2Index) * v2FormatInfoSize) // formats

	return extraData
}

func (b *indexBuilderV2) writeIndexEntry(w io.Writer, it *Info) error {
	var hashBuf [maxContentIDSize]byte

	k := contentIDToBytes(hashBuf[:0], it.ContentID)

	if len(k) != b.keyLength {
		return errors.Errorf("inconsistent key length: %v vs %v", len(k), b.keyLength)
	}

	if _, err := w.Write(k); err != nil {
		return errors.Wrap(err, "error writing entry key")
	}

	if err := b.writeIndexValueEntry(w, it); err != nil {
		return errors.Wrap(err, "error writing entry")
	}

	return nil
}

func (b *indexBuilderV2) writePackIDEntry(w io.Writer, packID blob.ID) error {
	var buf [v2PackInfoSize]byte

	buf[0] = byte(len(packID))
	binary.BigEndian.PutUint32(buf[1:], b.packBlobIDOffsets[packID]+b.extraDataOffset)

	_, err := w.Write(buf[:])

	return errors.Wrap(err, "error writing pack ID entry")
}

func (b *indexBuilderV2) writeFormatInfoEntry(w io.Writer, f indexV2FormatInfo) error {
	var buf [v2FormatInfoSize]byte

	binary.BigEndian.PutUint32(buf[v2FormatOffsetCompressionID:], uint32(f.compressionHeaderID))
	buf[v2FormatOffsetFormatVersion] = f.formatVersion
	buf[v2FormatOffsetEncryptionKeyID] = f.encryptionKeyID

	_, err := w.Write(buf[:])

	return errors.Wrap(err, "error writing format info entry")
}

func (b *indexBuilderV2) writeIndexValueEntry(w io.Writer, it *Info) error {
	var buf [v2EntryMaxLength]byte

	//    0-3: timestamp bits 0..31 (relative to base time)

	binary.BigEndian.PutUint32(
		buf[v2EntryOffsetTimestampSeconds:],
		uint32(it.TimestampSeconds-b.baseTimestamp)) //nolint:gosec

	//    4-7: pack offset bits 0..29
	//         flags:
	//            isDeleted                    (1 bit)

	packOffsetAndFlags := it.PackOffset
	if it.Deleted {
		packOffsetAndFlags |= v2DeletedMarker
	}

	binary.BigEndian.PutUint32(buf[v2EntryOffsetPackOffsetAndFlags:], packOffsetAndFlags)

	//   8-10: original length bits 0..23

	encodeBigEndianUint24(buf[v2EntryOffsetOriginalLength:], it.OriginalLength)

	//  11-13: packed length bits 0..23

	encodeBigEndianUint24(buf[v2EntryOffsetPackedLength:], it.PackedLength)

	//  14-15: pack ID (lower 16 bits)- index into Packs[]

	packBlobIndex := b.packID2Index[it.PackBlobID]
	binary.BigEndian.PutUint16(buf[v2EntryOffsetPackBlobID:], uint16(packBlobIndex)) //nolint:gosec

	//     16: format ID - index into Formats[] - 0 - present if not all formats are identical

	buf[v2EntryOffsetFormatID] = b.uniqueFormatInfo2Index[indexV2FormatInfoFromInfo(it)]

	//     17: pack ID - bits 16..23 - present if more than 2^16 packs are in a single index
	buf[v2EntryOffsetExtendedPackBlobID] = byte(packBlobIndex >> v2EntryExtendedPackBlobIDShift)

	//     18: high-order bits - present if any content length is greater than 2^24 == 16MiB
	//            original length bits 24..27  (4 hi bits)
	//            packed length bits 24..27    (4 lo bits)
	buf[v2EntryOffsetHighLengthBits] = byte(it.PackedLength>>v2EntryHighLengthShift) | byte((it.OriginalLength>>v2EntryHighLengthShift)<<v2EntryHighLengthBitsOriginalLengthShift)

	for i := b.entrySize; i < v2EntryMaxLength; i++ {
		if buf[i] != 0 {
			panic(fmt.Sprintf("encoding bug %x (entrySize=%v)", buf, b.entrySize))
		}
	}

	_, err := w.Write(buf[0:b.entrySize])

	return errors.Wrap(err, "error writing index value entry")
}

func openV2PackIndex(data []byte, closer func() error) (Index, error) {
	header, err := safeSlice(data, 0, v2IndexHeaderSize)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	hi := v2HeaderInfo{
		version:       int(header[0]),
		keySize:       int(header[1]),
		entrySize:     int(binary.BigEndian.Uint16(header[2:4])),
		entryCount:    int(binary.BigEndian.Uint32(header[4:8])),
		packCount:     uint(binary.BigEndian.Uint32(header[8:12])),
		formatCount:   header[12],
		baseTimestamp: binary.BigEndian.Uint32(header[13:17]),
	}

	if hi.keySize <= 1 || hi.entrySize < v2EntryMinLength || hi.entrySize > v2EntryMaxLength || hi.entryCount < 0 || hi.formatCount > v2MaxFormatCount {
		return nil, errors.New("invalid header")
	}

	hi.entryStride = int64(hi.keySize + hi.entrySize)
	if hi.entryStride > v2MaxEntrySize {
		return nil, errors.New("invalid header - entry stride too big")
	}

	hi.entriesOffset = v2IndexHeaderSize
	hi.packsOffset = hi.entriesOffset + int64(hi.entryCount)*hi.entryStride
	hi.formatsOffset = hi.packsOffset + int64(hi.packCount*v2PackInfoSize) //nolint:gosec

	// pre-read formats section
	formatsBuf, err := safeSlice(data, hi.formatsOffset, int(hi.formatCount)*v2FormatInfoSize)
	if err != nil {
		return nil, errors.New("unable to read formats section")
	}

	packIDs := make([]blob.ID, hi.packCount)

	for i := range int(hi.packCount) { //nolint:gosec
		buf, err := safeSlice(data, hi.packsOffset+int64(v2PackInfoSize*i), v2PackInfoSize)
		if err != nil {
			return nil, errors.New("unable to read pack blob IDs section - 1")
		}

		nameLength := int(buf[0])
		nameOffset := binary.BigEndian.Uint32(buf[1:])

		nameBuf, err := safeSliceString(data, int64(nameOffset), nameLength)
		if err != nil {
			return nil, errors.New("unable to read pack blob IDs section - 2")
		}

		packIDs[i] = blob.ID(nameBuf)
	}

	return &indexV2{
		hdr:         hi,
		data:        data,
		closer:      closer,
		formats:     parseFormatsBuffer(formatsBuf, int(hi.formatCount)),
		packBlobIDs: packIDs,
	}, nil
}

func parseFormatsBuffer(formatsBuf []byte, cnt int) []indexV2FormatInfo {
	formats := make([]indexV2FormatInfo, cnt)

	for i := range cnt {
		f := formatsBuf[v2FormatInfoSize*i:]

		formats[i].compressionHeaderID = compression.HeaderID(binary.BigEndian.Uint32(f[v2FormatOffsetCompressionID:]))
		formats[i].formatVersion = f[v2FormatOffsetFormatVersion]
		formats[i].encryptionKeyID = f[v2FormatOffsetEncryptionKeyID]
	}

	return formats
}
