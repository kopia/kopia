package index

import (
	"bytes"
	"fmt"
)

func bytesToContentID(b []byte) ID {
	if len(b) == 0 {
		return ID{}
	}

	if len(b) > maxIDLength+1 {
		panic(fmt.Sprintln("Content ID byte slice is longer than the maximum supported ID:", len(b)))
	}

	var id ID

	id.prefix = b[0]
	id.idLen = uint8(len(b) - 1) //nolint:gosec // len(b) is checked above
	copy(id.data[0:len(b)-1], b[1:])

	return id
}

func contentIDBytesGreaterOrEqual(a, b []byte) bool {
	return bytes.Compare(a, b) >= 0
}

func contentIDToBytes(output []byte, c ID) []byte {
	return append(append(output, c.prefix), c.data[0:c.idLen]...)
}
