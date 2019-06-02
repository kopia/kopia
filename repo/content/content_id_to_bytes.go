package content

import (
	"encoding/hex"
)

func bytesToContentID(b []byte) ID {
	if len(b) == 0 {
		return ""
	}
	if b[0] == 0xff {
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
		return append([]byte{0xff}, []byte(c)...)
	}

	return append(prefix, b...)
}
