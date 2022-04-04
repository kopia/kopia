package index

import (
	"bytes"
	"encoding/hex"

	"github.com/kopia/kopia/repo/hashing"
)

// unpackedContentIDPrefix is a prefix for all content IDs that are stored unpacked in the index.
const unpackedContentIDPrefix = 0xff

func bytesToContentID(b []byte) ID {
	if len(b) == 0 {
		return ""
	}

	if b[0] == unpackedContentIDPrefix {
		return ID(b[1:])
	}

	prefix := ""

	if b[0] != 0 {
		prefix = string(b[0:1])
	}

	return ID(prefix + hex.EncodeToString(b[1:]))
}

func contentIDBytesGreaterOrEqual(a, b []byte) bool {
	return bytes.Compare(a, b) >= 0
}

func contentIDToBytes(output []byte, c ID) []byte {
	var skip int

	if len(c)%2 == 1 {
		output = append(output, c[0])
		skip = 1
	} else {
		output = append(output, 0)
	}

	var hashBuf [hashing.MaxHashSize]byte

	n, err := hex.Decode(hashBuf[:], []byte(c[skip:]))
	if err != nil {
		// rare case
		return append([]byte{unpackedContentIDPrefix}, []byte(c)...)
	}

	return append(output, hashBuf[0:n]...)
}
