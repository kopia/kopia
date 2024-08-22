package hashing

import (
	"hash"

	"lukechampine.com/blake3"
)

const blake3KeySize = 32

func newBlake3(key []byte) (hash.Hash, error) {
	// Does the key need to be stretched?
	if len(key) < blake3KeySize {
		var xKey [blake3KeySize]byte

		blake3.DeriveKey(xKey[:blake3KeySize], "kopia blake3 derived key v1", key)
		key = xKey[:blake3KeySize]
	}

	return blake3.New(blake3KeySize, key[:blake3KeySize]), nil
}

func init() {
	Register("BLAKE3-256", truncatedKeyedHashFuncFactory(newBlake3, 32))     //nolint:mnd
	Register("BLAKE3-256-128", truncatedKeyedHashFuncFactory(newBlake3, 16)) //nolint:mnd
}
