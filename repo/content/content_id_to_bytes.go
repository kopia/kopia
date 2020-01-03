package content

import (
	"encoding/hex"
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

func contentIDToBytes(c ID) []byte {
	var prefix []byte

	var skip int

	if len(c)%2 == 1 {
		prefix = []byte(c[0:1])
		skip = 1
	} else {
		prefix = []byte{0}
	}

	b, err := hex.DecodeString(string(c[skip:]))
	if err != nil {
		return append([]byte{unpackedContentIDPrefix}, []byte(c)...)
	}

	return append(prefix, b...)
}
