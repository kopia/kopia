// Package ecc implements common support for error correction in sharded blob providers
package ecc

import (
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
)

// CreateECCFunc creates an ECC for given parameters.
type CreateECCFunc func(opts *Options) (encryption.Encryptor, error)

// nolint:gochecknoglobals
var factories = map[string]CreateECCFunc{}

// RegisterAlgorithm registers new ecc algorithm.
func RegisterAlgorithm(name string, createFunc CreateECCFunc) {
	factories[name] = createFunc
}

// SupportedAlgorithms returns the names of the supported ecc
// methods.
func SupportedAlgorithms() []string {
	var result []string

	for k := range factories {
		result = append(result, k)
	}

	sort.Strings(result)

	return result
}

// CreateAlgorithm returns new encryption.Encryptor with error correction.
func CreateAlgorithm(opts *Options) (encryption.Encryptor, error) {
	factory, exists := factories[opts.Algorithm]

	if !exists {
		return nil, errors.New("Unknown ECC algorithm: " + opts.Algorithm)
	}

	return factory(opts)
}

// New returns new encryption.Encryptor with error correction wrapped over another encryptor.
func New(next encryption.Encryptor, opts *Options) (encryption.Encryptor, error) {
	if opts.Algorithm == "" {
		return next, nil
	}

	impl, err := CreateAlgorithm(opts)
	if err != nil {
		return nil, err
	}

	return &encryptorWrapper{
		next: next,
		impl: impl,
	}, nil
}

type encryptorWrapper struct {
	next encryption.Encryptor
	impl encryption.Encryptor
}

func (e encryptorWrapper) Encrypt(plainText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := e.next.Encrypt(plainText, contentID, &tmp); err != nil {
		// nolint:wrapcheck
		return err
	}

	// nolint:wrapcheck
	return e.impl.Encrypt(tmp.Bytes(), contentID, output)
}

func (e encryptorWrapper) Decrypt(cipherText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := e.impl.Decrypt(cipherText, contentID, &tmp); err != nil {
		// nolint:wrapcheck
		return err
	}

	// nolint:wrapcheck
	return e.next.Decrypt(tmp.Bytes(), contentID, output)
}

func (e encryptorWrapper) Overhead() int {
	return e.impl.Overhead() + e.next.Overhead()
}
