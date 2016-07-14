package repo

import (
	"fmt"
	"io"

	"github.com/kopia/kopia/storage"
)

type seekTableEntry struct {
	startOffset int64
	length      int64
	blockID     string
}

func (r *seekTableEntry) endOffset() int64 {
	return r.startOffset + int64(r.length)
}

func (r *seekTableEntry) String() string {
	return fmt.Sprintf("start: %d len: %d end: %d block: %s",
		r.startOffset,
		r.length,
		r.endOffset(),
		r.blockID)
}

type objectReader struct {
	storage storage.Storage

	seekTable []seekTableEntry

	currentPosition int64 // Overall position in the objectReader
	totalLength     int64 // Overall length

	currentChunkIndex    int    // Index of current chunk in the seek table
	currentChunkData     []byte // Current chunk data
	currentChunkPosition int    // Read position in the current chunk
}

func (r *objectReader) Read(buffer []byte) (int, error) {
	readBytes := 0
	remaining := len(buffer)

	for remaining > 0 {
		if r.currentChunkData != nil {
			toCopy := len(r.currentChunkData) - r.currentChunkPosition
			if toCopy == 0 {
				// EOF on curren chunk
				r.closeCurrentChunk()
				r.currentChunkIndex++
				continue
			}

			if toCopy > remaining {
				toCopy = remaining
			}

			copy(buffer[readBytes:],
				r.currentChunkData[r.currentChunkPosition:r.currentChunkPosition+toCopy])
			r.currentChunkPosition += toCopy
			r.currentPosition += int64(toCopy)
			readBytes += toCopy
			remaining -= toCopy
		} else if r.currentChunkIndex < len(r.seekTable) {
			err := r.openCurrentChunk()
			if err != nil {
				return 0, err
			}
		} else {
			break
		}
	}

	if readBytes == 0 {
		return readBytes, io.EOF
	}

	return readBytes, nil
}

func (r *objectReader) openCurrentChunk() error {
	blockID := r.seekTable[r.currentChunkIndex].blockID
	blockData, err := r.storage.GetBlock(blockID)
	if err != nil {
		return err
	}

	r.currentChunkData = blockData
	r.currentChunkPosition = 0
	return nil
}

func (r *objectReader) closeCurrentChunk() {
	r.currentChunkData = nil
}

func (r *objectReader) findChunkIndexForOffset(offset int64) int {
	left := 0
	right := len(r.seekTable) - 1
	for left <= right {
		middle := (left + right) / 2

		if offset < r.seekTable[middle].startOffset {
			right = middle - 1
			continue
		}

		if offset >= r.seekTable[middle].endOffset() {
			left = middle + 1
			continue
		}

		return middle
	}

	panic("Unreachable code")
}

func (r *objectReader) Seek(offset int64, whence int) (int64, error) {
	if whence == 1 {
		return r.Seek(r.currentPosition+offset, 0)
	}

	if whence == 2 {
		return r.Seek(r.totalLength+offset, 0)
	}

	if offset < 0 {
		return -1, fmt.Errorf("Invalid seek.")
	}

	if offset > r.totalLength {
		offset = r.totalLength
	}

	index := r.findChunkIndexForOffset(offset)

	chunkStartOffset := r.seekTable[index].startOffset

	if index != r.currentChunkIndex {
		r.closeCurrentChunk()
		r.currentChunkIndex = index
	}

	if r.currentChunkData == nil {
		r.openCurrentChunk()
	}

	r.currentChunkPosition = int(offset - chunkStartOffset)
	r.currentPosition = offset

	return r.currentPosition, nil
}

func (r *objectReader) Close() error {
	return nil
}
