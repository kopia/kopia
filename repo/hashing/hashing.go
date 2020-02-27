// Package hashing encapsulates all keyed hashing algorithms.
package hashing

import (
	"crypto/hmac"
	"hash"
	"sort"

	"github.com/pkg/errors"
)

// Parameters encapsulates all hashing-relevant parameters.
type Parameters interface {
	GetHashFunction() string
	GetHMACSecret() []byte
}

// HashFunc computes hash of content of data using a cryptographic hash function, possibly with HMAC and/or truncation.
type HashFunc func(data []byte) []byte

// HashFuncFactory returns a hash function for given formatting options.
type HashFuncFactory func(p Parameters) (HashFunc, error)

var hashFunctions = map[string]HashFuncFactory{}

// Register registers a hash function with a given name.
func Register(name string, newHashFunc HashFuncFactory) {
	hashFunctions[name] = newHashFunc
}

// SupportedAlgorithms returns the names of the supported hashing schemes
func SupportedAlgorithms() []string {
	var result []string
	for k := range hashFunctions {
		result = append(result, k)
	}

	sort.Strings(result)

	return result
}

// DefaultAlgorithm is the name of the default hash algorithm.
const DefaultAlgorithm = "BLAKE2B-256-128"

// truncatedHMACHashFuncFactory returns a HashFuncFactory that computes HMAC(hash, secret) of a given content of bytes
// and truncates results to the given size.
func truncatedHMACHashFuncFactory(hf func() hash.Hash, truncate int) HashFuncFactory {
	return func(p Parameters) (HashFunc, error) {
		return func(b []byte) []byte {
			h := hmac.New(hf, p.GetHMACSecret())
			h.Write(b) // nolint:errcheck

			return h.Sum(nil)[0:truncate]
		}, nil
	}
}

// truncatedKeyedHashFuncFactory returns a HashFuncFactory that computes keyed hash of a given content of bytes
// and truncates results to the given size.
func truncatedKeyedHashFuncFactory(hf func(key []byte) (hash.Hash, error), truncate int) HashFuncFactory {
	return func(p Parameters) (HashFunc, error) {
		if _, err := hf(p.GetHMACSecret()); err != nil {
			return nil, err
		}

		return func(b []byte) []byte {
			h, _ := hf(p.GetHMACSecret())
			h.Write(b) // nolint:errcheck

			return h.Sum(nil)[0:truncate]
		}, nil
	}
}

// CreateHashFunc creates hash function from a given parameters.
func CreateHashFunc(p Parameters) (HashFunc, error) {
	h := hashFunctions[p.GetHashFunction()]
	if h == nil {
		return nil, errors.Errorf("unknown hash function %v", p.GetHashFunction())
	}

	hashFunc, err := h(p)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize hash")
	}

	if hashFunc == nil {
		return nil, errors.Errorf("nil hash function returned for %v", p.GetHashFunction())
	}

	return hashFunc, nil
}
