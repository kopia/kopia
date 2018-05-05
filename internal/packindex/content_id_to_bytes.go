package packindex

import (
	"encoding/hex"
)

func bytesToContentID(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if b[0] == 0xff {
		return string(b[1:])
	}
	prefix := ""
	if b[0] != 0 {
		prefix = string(b[0:1])
	}

	return prefix + hex.EncodeToString(b[1:])
}

func contentIDToBytes(c string) []byte {
	var prefix []byte
	if len(c)%2 == 1 {
		prefix = []byte(c[0:1])
		c = c[1:]
	} else {
		prefix = []byte{0}
	}

	b, err := hex.DecodeString(c)
	if err != nil {
		return append([]byte{0xff}, []byte(c)...)
	}

	return append(prefix, b...)
}
