package hashing

import (
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/blake2s"
)

func init() {
	Register("BLAKE2S-128", truncatedKeyedHashFuncFactory(blake2s.New128, 16))     //nolint:mnd
	Register("BLAKE2S-256", truncatedKeyedHashFuncFactory(blake2s.New256, 32))     //nolint:mnd
	Register("BLAKE2B-256-128", truncatedKeyedHashFuncFactory(blake2b.New256, 16)) //nolint:mnd
	Register("BLAKE2B-256", truncatedKeyedHashFuncFactory(blake2b.New256, 32))     //nolint:mnd
}
