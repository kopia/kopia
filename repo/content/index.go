package content

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// packIndex is a read-only index of packed contents.
type packIndex interface {
	io.Closer

	GetInfo(contentID ID) (*Info, error)
	Iterate(prefix ID, cb func(Info) error) error
}

type index struct {
	hdr      headerInfo
	readerAt io.ReaderAt
}

type headerInfo struct {
	keySize    int
	valueSize  int
	entryCount int
}

func readHeader(readerAt io.ReaderAt) (headerInfo, error) {
	var header [8]byte

	if n, err := readerAt.ReadAt(header[:], 0); err != nil || n != 8 {
		return headerInfo{}, errors.Wrap(err, "invalid header")
	}

	if header[0] != 1 {
		return headerInfo{}, errors.Errorf("invalid header format: %v", header[0])
	}

	hi := headerInfo{
		keySize:    int(header[1]),
		valueSize:  int(binary.BigEndian.Uint16(header[2:4])),
		entryCount: int(binary.BigEndian.Uint32(header[4:8])),
	}

	if hi.keySize <= 1 || hi.valueSize < 0 || hi.entryCount < 0 {
		return headerInfo{}, errors.Errorf("invalid header")
	}

	return hi, nil
}

// Iterate invokes the provided callback function for all contents in the index, sorted alphabetically.
// The iteration ends when the callback returns an error, which is propagated to the caller or when
// all contents have been visited.
func (b *index) Iterate(prefix ID, cb func(Info) error) error {
	startPos, err := b.findEntryPosition(prefix)
	if err != nil {
		return errors.Wrap(err, "could not find starting position")
	}
	stride := b.hdr.keySize + b.hdr.valueSize
	entry := make([]byte, stride)
	for i := startPos; i < b.hdr.entryCount; i++ {
		n, err := b.readerAt.ReadAt(entry, int64(8+stride*i))
		if err != nil || n != len(entry) {
			return errors.Wrap(err, "unable to read from index")
		}

		key := entry[0:b.hdr.keySize]
		value := entry[b.hdr.keySize:]

		i, err := b.entryToInfo(bytesToContentID(key), value)
		if err != nil {
			return errors.Wrap(err, "invalid index data")
		}
		if !strings.HasPrefix(string(i.ID), string(prefix)) {
			break
		}
		if err := cb(i); err != nil {
			return err
		}
	}
	return nil
}

func (b *index) findEntryPosition(contentID ID) (int, error) {
	stride := b.hdr.keySize + b.hdr.valueSize
	entryBuf := make([]byte, stride)
	var readErr error
	pos := sort.Search(b.hdr.entryCount, func(p int) bool {
		if readErr != nil {
			return false
		}
		_, err := b.readerAt.ReadAt(entryBuf, int64(8+stride*p))
		if err != nil {
			readErr = err
			return false
		}

		return bytesToContentID(entryBuf[0:b.hdr.keySize]) >= contentID
	})

	return pos, readErr
}

func (b *index) findEntry(contentID ID) ([]byte, error) {
	key := contentIDToBytes(contentID)
	if len(key) != b.hdr.keySize {
		return nil, errors.Errorf("invalid content ID: %q", contentID)
	}
	stride := b.hdr.keySize + b.hdr.valueSize

	position, err := b.findEntryPosition(contentID)
	if err != nil {
		return nil, err
	}
	if position >= b.hdr.entryCount {
		return nil, nil
	}

	entryBuf := make([]byte, stride)
	if _, err := b.readerAt.ReadAt(entryBuf, int64(8+stride*position)); err != nil {
		return nil, err
	}

	if bytes.Equal(entryBuf[0:len(key)], key) {
		return entryBuf[len(key):], nil
	}

	return nil, nil
}

// GetInfo returns information about a given content. If a content is not found, nil is returned.
func (b *index) GetInfo(contentID ID) (*Info, error) {
	e, err := b.findEntry(contentID)
	if err != nil {
		return nil, err
	}

	if e == nil {
		return nil, nil
	}

	i, err := b.entryToInfo(contentID, e)
	if err != nil {
		return nil, err
	}
	return &i, err
}

func (b *index) entryToInfo(contentID ID, entryData []byte) (Info, error) {
	if len(entryData) < 20 {
		return Info{}, errors.Errorf("invalid entry length: %v", len(entryData))
	}

	var e entry
	if err := e.parse(entryData); err != nil {
		return Info{}, err
	}

	packFile := make([]byte, e.PackFileLength())
	n, err := b.readerAt.ReadAt(packFile, int64(e.PackFileOffset()))
	if err != nil || n != int(e.PackFileLength()) {
		return Info{}, errors.Wrap(err, "can't read pack content ID")
	}

	return Info{
		ID:               contentID,
		Deleted:          e.IsDeleted(),
		TimestampSeconds: e.TimestampSeconds(),
		FormatVersion:    e.PackedFormatVersion(),
		PackOffset:       e.PackedOffset(),
		Length:           e.PackedLength(),
		PackBlobID:       blob.ID(packFile),
	}, nil
}

// Close closes the index and the underlying reader.
func (b *index) Close() error {
	if closer, ok := b.readerAt.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

// openPackIndex reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func openPackIndex(readerAt io.ReaderAt) (packIndex, error) {
	h, err := readHeader(readerAt)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}
	return &index{hdr: h, readerAt: readerAt}, nil
}
