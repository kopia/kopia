// Package compression manages compression algorithm implementations.
package compression

import (
	"encoding/binary"
	"fmt"
)

// Name is the name of the compressor to use.
type Name string

// Compressor implements compression and decompression of a byte slice.
type Compressor interface {
	HeaderID() HeaderID
	Compress(b []byte) ([]byte, error)
	Decompress(b []byte) ([]byte, error)
}

// maps of registered compressors by header ID and name.
var (
	ByHeaderID = map[HeaderID]Compressor{}
	ByName     = map[Name]Compressor{}
)

// RegisterCompressor registers the provided compressor implementation
func RegisterCompressor(name Name, c Compressor) {
	if ByHeaderID[c.HeaderID()] != nil {
		panic(fmt.Sprintf("compressor with HeaderID %x already registered", c.HeaderID()))
	}

	if ByName[name] != nil {
		panic(fmt.Sprintf("compressor with name %q already registered", name))
	}

	ByHeaderID[c.HeaderID()] = c
	ByName[name] = c
}

func compressionHeader(id HeaderID) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(id))

	return b
}
