package packindex

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// Builder prepares and writes block index for writing.
type Builder map[ContentID]*Info

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b Builder) Add(i Info) {
	old, ok := b[i.BlockID]
	if !ok || i.TimestampSeconds >= old.TimestampSeconds {
		b[i.BlockID] = &i
	}
}

func (b Builder) sortedBlocks() []*Info {
	var allBlocks []*Info

	for _, v := range b {
		allBlocks = append(allBlocks, v)
	}

	sort.Slice(allBlocks, func(i, j int) bool {
		return allBlocks[i].BlockID < allBlocks[j].BlockID
	})

	return allBlocks
}

type indexLayout struct {
	packBlockIDOffsets map[PhysicalBlockID]uint32
	payloadOffsets     map[ContentID]uint32
	entryCount         int
	keyLength          int
	entryLength        int
	extraDataOffset    uint32
}

// Build writes the pack index to the provided output.
func (b Builder) Build(output io.Writer) error {
	allBlocks := b.sortedBlocks()
	layout := &indexLayout{
		packBlockIDOffsets: map[PhysicalBlockID]uint32{},
		payloadOffsets:     map[ContentID]uint32{},
		keyLength:          -1,
		entryLength:        20,
		entryCount:         len(allBlocks),
	}

	w := bufio.NewWriter(output)

	// prepare extra data to be appended at the end of an index.
	extraData := prepareExtraData(allBlocks, layout)

	// write header
	header := make([]byte, 8)
	header[0] = 1 // version
	header[1] = byte(layout.keyLength)
	binary.BigEndian.PutUint16(header[2:4], uint16(layout.entryLength))
	binary.BigEndian.PutUint32(header[4:8], uint32(layout.entryCount))
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("unable to write header: %v", err)
	}

	// write all sorted blocks.
	entry := make([]byte, layout.entryLength)
	for _, it := range allBlocks {
		if err := writeEntry(w, it, layout, entry); err != nil {
			return fmt.Errorf("unable to write entry: %v", err)
		}
	}

	if _, err := w.Write(extraData); err != nil {
		return fmt.Errorf("error writing extra data: %v", err)
	}

	return w.Flush()
}

func prepareExtraData(allBlocks []*Info, layout *indexLayout) []byte {
	var extraData []byte

	for i, it := range allBlocks {
		if i == 0 {
			layout.keyLength = len(contentIDToBytes(it.BlockID))
		}
		if it.PackBlockID != "" {
			if _, ok := layout.packBlockIDOffsets[it.PackBlockID]; !ok {
				layout.packBlockIDOffsets[it.PackBlockID] = uint32(len(extraData))
				extraData = append(extraData, []byte(it.PackBlockID)...)
			}
		}
		if len(it.Payload) > 0 {
			if _, ok := layout.payloadOffsets[it.BlockID]; !ok {
				layout.payloadOffsets[it.BlockID] = uint32(len(extraData))
				extraData = append(extraData, it.Payload...)
			}
		}
	}
	layout.extraDataOffset = uint32(8 + layout.entryCount*(layout.keyLength+layout.entryLength))
	return extraData
}

func writeEntry(w io.Writer, it *Info, layout *indexLayout, entry []byte) error {
	k := contentIDToBytes(it.BlockID)
	if len(k) != layout.keyLength {
		return fmt.Errorf("inconsistent key length: %v vs %v", len(k), layout.keyLength)
	}

	if err := formatEntry(entry, it, layout); err != nil {
		return fmt.Errorf("unable to format entry: %v", err)
	}

	if _, err := w.Write(k); err != nil {
		return fmt.Errorf("error writing entry key: %v", err)
	}
	if _, err := w.Write(entry); err != nil {
		return fmt.Errorf("error writing entry: %v", err)
	}

	return nil
}

func formatEntry(entry []byte, it *Info, layout *indexLayout) error {
	entryTimestampAndFlags := entry[0:8]
	entryOffset1 := entry[8:12]
	entryOffset2 := entry[12:16]
	entryLength1 := entry[16:20]
	timestampAndFlags := uint64(it.TimestampSeconds) << 16

	for i := 0; i < len(entry); i++ {
		entry[i] = 0
	}
	if it.Deleted {
		binary.BigEndian.PutUint32(entryOffset1, 0)
	} else if len(it.Payload) > 0 {
		binary.BigEndian.PutUint32(entryOffset1, 1)
		binary.BigEndian.PutUint32(entryOffset2, layout.extraDataOffset+layout.payloadOffsets[it.BlockID])
		binary.BigEndian.PutUint32(entryLength1, uint32(len(it.Payload)))
	} else {
		if len(it.PackBlockID) == 0 {
			return fmt.Errorf("empty pack block ID for %v", it.BlockID)
		}
		binary.BigEndian.PutUint32(entryOffset1, layout.extraDataOffset+layout.packBlockIDOffsets[it.PackBlockID])
		binary.BigEndian.PutUint32(entryOffset2, it.PackOffset)
		binary.BigEndian.PutUint32(entryLength1, it.Length)
		timestampAndFlags |= uint64(it.FormatVersion) << 8
		timestampAndFlags |= uint64(len(it.PackBlockID))
	}
	binary.BigEndian.PutUint64(entryTimestampAndFlags, timestampAndFlags)
	return nil
}

// NewBuilder creates a new Builder.
func NewBuilder() Builder {
	return make(map[ContentID]*Info)
}
