// Package compression manages compression algorithm implementations.
package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

const compressionHeaderSize = 4

// Name is the name of the compressor to use.
type Name string

// Compressor implements compression and decompression of a byte slice.
type Compressor interface {
	HeaderID() HeaderID
	Compress(output io.Writer, input io.Reader) error
	Decompress(output io.Writer, input io.Reader, withHeader bool) error
}

// maps of registered compressors by header ID and name.
//
//nolint:gochecknoglobals
var (
	ByHeaderID     = map[HeaderID]Compressor{}
	ByName         = map[Name]Compressor{}
	HeaderIDToName = map[HeaderID]Name{}
	IsDeprecated   = map[Name]bool{}
)

// RegisterCompressor registers the provided compressor implementation.
func RegisterCompressor(name Name, c Compressor) {
	if ByHeaderID[c.HeaderID()] != nil {
		panic(fmt.Sprintf("compressor with HeaderID %x already registered", c.HeaderID()))
	}

	if ByName[name] != nil {
		panic(fmt.Sprintf("compressor with name %q already registered", name))
	}

	ByHeaderID[c.HeaderID()] = c
	ByName[name] = c
	HeaderIDToName[c.HeaderID()] = name
}

// RegisterDeprecatedCompressor registers the provided compressor implementation.
func RegisterDeprecatedCompressor(name Name, c Compressor) {
	RegisterCompressor(name, c)

	IsDeprecated[name] = true
}

func compressionHeader(id HeaderID) []byte {
	b := make([]byte, compressionHeaderSize)
	binary.BigEndian.PutUint32(b, uint32(id))

	return b
}

// DecompressByHeader decodes compression header from the provided input and decompresses the remainder.
func DecompressByHeader(output io.Writer, input io.Reader) error {
	var b [compressionHeaderSize]byte

	if _, err := io.ReadFull(input, b[:]); err != nil {
		return errors.Wrap(err, "error reading compression header")
	}

	compressorID := HeaderID(binary.BigEndian.Uint32(b[0:compressionHeaderSize]))

	compressor := ByHeaderID[compressorID]
	if compressor == nil {
		return errors.Errorf("unsupported compressor %x", compressorID)
	}

	return errors.Wrap(compressor.Decompress(output, input, false), "error decompressing")
}

func mustSucceed(err error) {
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
}

func verifyCompressionHeader(reader io.Reader, want []byte) error {
	var actual [compressionHeaderSize]byte

	if _, err := io.ReadFull(reader, actual[:]); err != nil {
		return errors.Wrap(err, "error reading compression header")
	}

	if !bytes.Equal(actual[:], want) {
		return errors.Errorf("invalid compression header, expected %x but got %x", want, actual[:])
	}

	return nil
}
