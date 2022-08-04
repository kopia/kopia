// Package ecc implements common support for error correction in sharded blob providers
package ecc

import (
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/pkg/errors"
	"sort"
)

type CreateEccFunc func(opts *Options) (encryption.Encryptor, error)

var factories = map[string]CreateEccFunc{}

func RegisterAlgorithm(name string, createFunc CreateEccFunc) {
	factories[name] = createFunc
}

func SupportedAlgorithms() []string {
	var result []string

	for k, _ := range factories {
		result = append(result, k)
	}

	sort.Strings(result)

	return result
}

func CreateAlgorithm(opts *Options) (encryption.Encryptor, error) {
	factory, exists := factories[opts.Algorithm]

	if !exists {
		return nil, errors.New("Unknown ECC algorithm: " + opts.Algorithm)
	}

	return factory(opts)
}

// New returns new encryption.Encryptor with error correction.
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

	if err := e.impl.Encrypt(plainText, contentID, &tmp); err != nil {
		return err
	}

	return e.next.Encrypt(tmp.Bytes(), contentID, output)
}

func (e encryptorWrapper) Decrypt(cipherText gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := e.next.Decrypt(cipherText, contentID, &tmp); err != nil {
		return err
	}

	return e.impl.Decrypt(tmp.Bytes(), contentID, output)
}

func (e encryptorWrapper) Overhead() int {
	return e.impl.Overhead() + e.next.Overhead()
}
