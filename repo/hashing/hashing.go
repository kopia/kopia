// Package hashing encapsulates all keyed hashing algorithms.
package hashing

import (
	"crypto/hmac"
	"hash"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

// MaxHashSize is the maximum hash size supported in the system.
const MaxHashSize = 32

// Parameters encapsulates all hashing-relevant parameters.
type Parameters interface {
	GetHashFunction() string
	GetHmacSecret() []byte
}

// HashFunc computes hash of content of data using a cryptographic hash function, possibly with HMAC and/or truncation.
type HashFunc func(output []byte, data gather.Bytes) []byte

// HashFuncFactory returns a hash function for given formatting options.
type HashFuncFactory func(p Parameters) (HashFunc, error)

//nolint:gochecknoglobals
var hashFunctions = map[string]HashFuncFactory{}

// Register registers a hash function with a given name.
func Register(name string, newHashFunc HashFuncFactory) {
	hashFunctions[name] = newHashFunc
}

// SupportedAlgorithms returns the names of the supported hashing schemes.
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
		pool := sync.Pool{
			New: func() interface{} {
				return hmac.New(hf, p.GetHmacSecret())
			},
		}

		return func(output []byte, data gather.Bytes) []byte {
			//nolint:forcetypeassert
			h := pool.Get().(hash.Hash)
			defer pool.Put(h)

			h.Reset()
			data.WriteTo(h) //nolint:errcheck

			return h.Sum(output)[0:truncate]
		}, nil
	}
}

// truncatedKeyedHashFuncFactory returns a HashFuncFactory that computes keyed hash of a given content of bytes
// and truncates results to the given size.
func truncatedKeyedHashFuncFactory(hf func(key []byte) (hash.Hash, error), truncate int) HashFuncFactory {
	return func(p Parameters) (HashFunc, error) {
		secret := p.GetHmacSecret()
		if _, err := hf(secret); err != nil {
			return nil, err
		}

		pool := sync.Pool{
			New: func() interface{} {
				h, _ := hf(secret)
				return h
			},
		}

		return func(output []byte, data gather.Bytes) []byte {
			//nolint:forcetypeassert
			h := pool.Get().(hash.Hash)
			defer pool.Put(h)

			h.Reset()
			data.WriteTo(h) //nolint:errcheck

			return h.Sum(output)[0:truncate]
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
