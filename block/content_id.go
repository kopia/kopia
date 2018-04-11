package block

import (
	"encoding/hex"
	"fmt"
)

// ContentID uniquely identifies a block of content stored in repository.
// It consists of optional one-character prefix (which can't be 0..9 or a..f) followed by hexa-decimal
// digits representing hash of the content.
type ContentID string

func packContentID(c ContentID) ([]byte, error) {
	if len(c) < 2 {
		return nil, fmt.Errorf("invalid content ID: %q", c)
	}

	var hexDigits ContentID
	var prefix byte

	if !isHex(c[0]) {
		hexDigits = c[1:]
		prefix = c[0]
	} else {
		hexDigits = c
		prefix = 0
	}

	result := make([]byte, 1+len(hexDigits)/2)
	result[0] = prefix
	if _, err := hex.Decode(result[1:], []byte(hexDigits)); err != nil {
		return nil, fmt.Errorf("unable to decode content hash: %v", err)
	}

	return result, nil
}

func unpackContentID(b []byte) (ContentID, error) {
	if len(b) <= 1 {
		return "", fmt.Errorf("invalid content ID: %x", b)
	}

	var prefix string
	if b[0] != 0 {
		prefix = string(b[0:1])
	}
	return ContentID(prefix + hex.EncodeToString(b[1:])), nil
}

func isHex(b byte) bool {
	if b >= '0' && b <= '9' {
		return true
	}
	if b >= 'a' && b <= 'f' {
		return true
	}

	return false
}
