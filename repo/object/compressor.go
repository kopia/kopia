package object

import (
	"encoding/binary"
	"fmt"
)

// CompressorName is the name of the compressor to use.
type CompressorName string

type Compressor interface {
	ID() uint32
	Compress(b []byte) ([]byte, error)
	Decompress(b []byte) ([]byte, error)
}

var (
	Compressors       = map[uint32]Compressor{}
	CompressorsByName = map[CompressorName]Compressor{}
)

// RegisterCompressor registers the provided compressor implementation
func RegisterCompressor(name CompressorName, c Compressor) {
	if Compressors[c.ID()] != nil {
		panic(fmt.Sprintf("compressor with ID %x already registered", c.ID()))
	}

	if CompressorsByName[name] != nil {
		panic(fmt.Sprintf("compressor with name %q already registered", name))
	}

	Compressors[c.ID()] = c
	CompressorsByName[name] = c
}

func compressionHeader(id uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, id)

	return b
}
