package object

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

func (i *indirectObjectEntry) endOffset() int64 {
	return i.Start + i.Length
}

type objectReader struct {
	ctx  context.Context
	repo *Manager

	seekTable []indirectObjectEntry

	currentPosition int64 // Overall position in the objectReader
	totalLength     int64 // Overall length

	currentChunkIndex    int    // Index of current chunk in the seek table
	currentChunkData     []byte // Current chunk data
	currentChunkPosition int    // Read position in the current chunk
}

func (r *objectReader) Read(buffer []byte) (int, error) {
	readBytes := 0
	remaining := len(buffer)

	if r.currentPosition >= r.totalLength {
		return 0, io.EOF
	}

	for remaining > 0 {
		if r.currentChunkData != nil {
			toCopy := len(r.currentChunkData) - r.currentChunkPosition
			if toCopy == 0 {
				// EOF on current chunk
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

			continue
		}

		if r.currentChunkIndex < len(r.seekTable) {
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
	st := r.seekTable[r.currentChunkIndex]

	rd, err := r.repo.openAndAssertLength(r.ctx, st.Object, st.Length)
	if err != nil {
		return err
	}

	defer rd.Close() //nolint:errcheck

	b := make([]byte, st.Length)
	if _, err := io.ReadFull(rd, b); err != nil {
		return err
	}

	r.currentChunkData = b
	r.currentChunkPosition = 0

	return nil
}

func (r *objectReader) closeCurrentChunk() {
	r.currentChunkData = nil
}

func (r *objectReader) findChunkIndexForOffset(offset int64) (int, error) {
	left := 0
	right := len(r.seekTable) - 1

	for left <= right {
		middle := (left + right) / 2

		if offset < r.seekTable[middle].Start {
			right = middle - 1
			continue
		}

		if offset >= r.seekTable[middle].endOffset() {
			left = middle + 1
			continue
		}

		return middle, nil
	}

	return 0, errors.Errorf("can't find chunk for offset %v", offset)
}

func (r *objectReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		return r.Seek(r.currentPosition+offset, 0)
	}

	if whence == io.SeekEnd {
		return r.Seek(r.totalLength+offset, 0)
	}

	if offset >= r.totalLength {
		r.currentChunkIndex = len(r.seekTable)
		r.currentChunkData = nil
		r.currentPosition = offset

		return offset, nil
	}

	index, err := r.findChunkIndexForOffset(offset)
	if err != nil {
		return -1, errors.Wrapf(err, "invalid seek %v %v", offset, whence)
	}

	chunkStartOffset := r.seekTable[index].Start

	if index != r.currentChunkIndex {
		r.closeCurrentChunk()
		r.currentChunkIndex = index
	}

	if r.currentChunkData == nil {
		if err := r.openCurrentChunk(); err != nil {
			return 0, err
		}
	}

	r.currentChunkPosition = int(offset - chunkStartOffset)
	r.currentPosition = offset

	return r.currentPosition, nil
}

func (r *objectReader) Close() error {
	return nil
}

func (r *objectReader) Length() int64 {
	return r.totalLength
}
