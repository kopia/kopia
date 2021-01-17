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
type packIndexBuilder map[ID]*Info

// clone returns a deep clone of packIndexBuilder.
func (b packIndexBuilder) clone() packIndexBuilder {
	if b == nil {
		return nil
	}

	r := packIndexBuilder{}

	for k, v := range b {
		i2 := *v
		r[k] = &i2
	}

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b packIndexBuilder) Add(i Info) {
	old, ok := b[i.ID]
	if !ok || i.TimestampSeconds >= old.TimestampSeconds {
		b[i.ID] = &i
	}
}

func (b packIndexBuilder) sortedContents() []*Info {
	var allContents []*Info

	for _, v := range b {
		allContents = append(allContents, v)
	}

	sort.Slice(allContents, func(i, j int) bool {
		return allContents[i].ID < allContents[j].ID
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

func prepareExtraData(allContents []*Info, layout *indexLayout) []byte {
	var extraData []byte

	var hashBuf [maxContentIDSize]byte

	for i, it := range allContents {
		if i == 0 {
			layout.keyLength = len(contentIDToBytes(hashBuf[:0], it.ID))
		}

		if it.PackBlobID != "" {
			if _, ok := layout.packBlobIDOffsets[it.PackBlobID]; !ok {
				layout.packBlobIDOffsets[it.PackBlobID] = uint32(len(extraData))
				extraData = append(extraData, []byte(it.PackBlobID)...)
			}
		}
	}

	layout.extraDataOffset = uint32(packHeaderSize + layout.entryCount*(layout.keyLength+layout.entryLength))

	return extraData
}

func writeEntry(w io.Writer, it *Info, layout *indexLayout, entry []byte) error {
	var hashBuf [maxContentIDSize]byte

	k := contentIDToBytes(hashBuf[:0], it.ID)

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

func formatEntry(entry []byte, it *Info, layout *indexLayout) error {
	entryTimestampAndFlags := entry[0:8]
	entryPackFileOffset := entry[8:12]
	entryPackedOffset := entry[12:16]
	entryPackedLength := entry[16:20]
	timestampAndFlags := uint64(it.TimestampSeconds) << 16 // nolint:gomnd

	if len(it.PackBlobID) == 0 {
		return errors.Errorf("empty pack content ID for %v", it.ID)
	}

	binary.BigEndian.PutUint32(entryPackFileOffset, layout.extraDataOffset+layout.packBlobIDOffsets[it.PackBlobID])

	if it.Deleted {
		binary.BigEndian.PutUint32(entryPackedOffset, it.PackOffset|deletedMarker)
	} else {
		binary.BigEndian.PutUint32(entryPackedOffset, it.PackOffset)
	}

	binary.BigEndian.PutUint32(entryPackedLength, it.Length)
	timestampAndFlags |= uint64(it.FormatVersion) << 8 // nolint:gomnd
	timestampAndFlags |= uint64(len(it.PackBlobID))
	binary.BigEndian.PutUint64(entryTimestampAndFlags, timestampAndFlags)

	return nil
}
