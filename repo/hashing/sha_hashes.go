package hashing

import (
	"crypto/sha256"
	"crypto/sha3"
	"hash"
)

func init() {
	Register("HMAC-SHA256", truncatedHMACHashFuncFactory(sha256.New, 32))                                  //nolint:mnd
	Register("HMAC-SHA256-128", truncatedHMACHashFuncFactory(sha256.New, 16))                              //nolint:mnd
	Register("HMAC-SHA224", truncatedHMACHashFuncFactory(sha256.New224, 28))                               //nolint:mnd
	Register("HMAC-SHA3-224", truncatedHMACHashFuncFactory(func() hash.Hash { return sha3.New224() }, 28)) //nolint:mnd
	Register("HMAC-SHA3-256", truncatedHMACHashFuncFactory(func() hash.Hash { return sha3.New256() }, 32)) //nolint:mnd
}
