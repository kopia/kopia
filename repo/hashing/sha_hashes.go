package hashing

import (
	"crypto/sha256"

	"golang.org/x/crypto/sha3"
)

func init() {
	Register("HMAC-SHA256", truncatedHMACHashFuncFactory(sha256.New, 32))     //nolint:mnd
	Register("HMAC-SHA256-128", truncatedHMACHashFuncFactory(sha256.New, 16)) //nolint:mnd
	Register("HMAC-SHA224", truncatedHMACHashFuncFactory(sha256.New224, 28))  //nolint:mnd
	Register("HMAC-SHA3-224", truncatedHMACHashFuncFactory(sha3.New224, 28))  //nolint:mnd
	Register("HMAC-SHA3-256", truncatedHMACHashFuncFactory(sha3.New256, 32))  //nolint:mnd
}
