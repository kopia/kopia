package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	cacheEntryFormatVersion = 1
	sizeDoesNotExists       = 0x4000000000000000
	sizeUnknown             = 0x4000000000000001
)

type blockCacheEntry struct {
	accessTime int64
	size       int64
}

func (e *blockCacheEntry) exists() bool {
	return e.size != sizeDoesNotExists
}

func (e *blockCacheEntry) isKnownSize() bool {
	return e.size != sizeUnknown
}

func (e *blockCacheEntry) serialize() []byte {
	var buf bytes.Buffer

	buf.WriteByte(cacheEntryFormatVersion)
	binary.Write(&buf, binary.BigEndian, e.accessTime)
	binary.Write(&buf, binary.BigEndian, e.size)

	return buf.Bytes()
}

func (e *blockCacheEntry) deserialize(b []byte) error {
	r := bytes.NewReader(b)

	var version byte
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return err
	}

	if version != cacheEntryFormatVersion {
		return errors.New("invalid format")
	}

	if err := binary.Read(r, binary.BigEndian, &e.accessTime); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &e.size); err != nil {
		return err
	}

	return nil
}
