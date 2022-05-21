package index

import (
	"bytes"
)

func bytesToContentID(b []byte) ID {
	if len(b) == 0 {
		return ID{}
	}

	var id ID

	id.prefix = b[0]
	id.idLen = byte(len(b) - 1)
	copy(id.data[0:len(b)-1], b[1:])

	return id
}

func contentIDBytesGreaterOrEqual(a, b []byte) bool {
	return bytes.Compare(a, b) >= 0
}

func contentIDToBytes(output []byte, c ID) []byte {
	return append(append(output, c.prefix), c.data[0:c.idLen]...)
}
