package content

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"io"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const (
	packHeaderSize = 8
	deletedMarker  = 0x80000000

	entryFixedHeaderLength = 20
	randomSuffixSize       = 32
)

// packIndexBuilder prepares and writes content index.
type packIndexBuilder map[ID]Info

// clone returns a deep clone of packIndexBuilder.
func (b packIndexBuilder) clone() packIndexBuilder {
	if b == nil {
		return nil
	}

	r := packIndexBuilder{}

	for k, v := range b {
		r[k] = v
	}

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b packIndexBuilder) Add(i Info) {
	old, ok := b[i.GetContentID()]
	if !ok || i.GetTimestampSeconds() >= old.GetTimestampSeconds() {
		b[i.GetContentID()] = i
	}
}

func (b packIndexBuilder) sortedContents() []Info {
	var allContents []Info

	for _, v := range b {
		allContents = append(allContents, v)
	}

	sort.Slice(allContents, func(i, j int) bool {
		return allContents[i].GetContentID() < allContents[j].GetContentID()
	})

	return allContents
}

type indexLayout struct {
	packBlobIDOffsets map[blob.ID]uint32
	entryCount        int
	keyLength         int
	entryLength       int
	extraDataOffset   uint32
}

// Build writes the pack index to the provided output.
func (b packIndexBuilder) Build(output io.Writer) error {
	allContents := b.sortedContents()
	layout := &indexLayout{
		packBlobIDOffsets: map[blob.ID]uint32{},
		keyLength:         -1,
		entryLength:       entryFixedHeaderLength,
		entryCount:        len(allContents),
	}

	w := bufio.NewWriter(output)

	// prepare extra data to be appended at the end of an index.
	extraData := prepareExtraData(allContents, layout)

	// write header
	header := make([]byte, packHeaderSize)
	header[0] = 1 // version
	header[1] = byte(layout.keyLength)
	binary.BigEndian.PutUint16(header[2:4], uint16(layout.entryLength))
	binary.BigEndian.PutUint32(header[4:8], uint32(layout.entryCount))

	if _, err := w.Write(header); err != nil {
		return errors.Wrap(err, "unable to write header")
	}

	// write all sorted contents.
	entry := make([]byte, layout.entryLength)

	for _, it := range allContents {
		if err := writeEntry(w, it, layout, entry); err != nil {
			return errors.Wrap(err, "unable to write entry")
		}
	}

	if _, err := w.Write(extraData); err != nil {
		return errors.Wrap(err, "error writing extra data")
	}

	randomSuffix := make([]byte, randomSuffixSize)
	if _, err := rand.Read(randomSuffix); err != nil {
		return errors.Wrap(err, "error getting random bytes for suffix")
	}

	if _, err := w.Write(randomSuffix); err != nil {
		return errors.Wrap(err, "error writing extra random suffix to ensure indexes are always globally unique")
	}

	return w.Flush()
}

func prepareExtraData(allContents []Info, layout *indexLayout) []byte {
	var extraData []byte

	var hashBuf [maxContentIDSize]byte

	for i, it := range allContents {
		if i == 0 {
			layout.keyLength = len(contentIDToBytes(hashBuf[:0], it.GetContentID()))
		}

		if it.GetPackBlobID() != "" {
			if _, ok := layout.packBlobIDOffsets[it.GetPackBlobID()]; !ok {
				layout.packBlobIDOffsets[it.GetPackBlobID()] = uint32(len(extraData))
				extraData = append(extraData, []byte(it.GetPackBlobID())...)
			}
		}
	}

	layout.extraDataOffset = uint32(packHeaderSize + layout.entryCount*(layout.keyLength+layout.entryLength))

	return extraData
}

func writeEntry(w io.Writer, it Info, layout *indexLayout, entry []byte) error {
	var hashBuf [maxContentIDSize]byte

	k := contentIDToBytes(hashBuf[:0], it.GetContentID())

	if len(k) != layout.keyLength {
		return errors.Errorf("inconsistent key length: %v vs %v", len(k), layout.keyLength)
	}

	if err := formatEntry(entry, it, layout); err != nil {
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

func formatEntry(entry []byte, it Info, layout *indexLayout) error {
	entryTimestampAndFlags := entry[0:8]
	entryPackFileOffset := entry[8:12]
	entryPackedOffset := entry[12:16]
	entryPackedLength := entry[16:20]
	timestampAndFlags := uint64(it.GetTimestampSeconds()) << 16 // nolint:gomnd

	packBlobID := it.GetPackBlobID()
	if len(packBlobID) == 0 {
		return errors.Errorf("empty pack content ID for %v", it.GetContentID())
	}

	binary.BigEndian.PutUint32(entryPackFileOffset, layout.extraDataOffset+layout.packBlobIDOffsets[packBlobID])

	if it.GetDeleted() {
		binary.BigEndian.PutUint32(entryPackedOffset, it.GetPackOffset()|deletedMarker)
	} else {
		binary.BigEndian.PutUint32(entryPackedOffset, it.GetPackOffset())
	}

	binary.BigEndian.PutUint32(entryPackedLength, it.GetPackedLength())
	timestampAndFlags |= uint64(it.GetFormatVersion()) << 8 // nolint:gomnd
	timestampAndFlags |= uint64(len(packBlobID))
	binary.BigEndian.PutUint64(entryTimestampAndFlags, timestampAndFlags)

	return nil
}
