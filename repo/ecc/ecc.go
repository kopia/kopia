// Package ecc implements common support for error correction in sharded blob providers
package ecc

import (
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/encryption"
)

// CreateECCFunc creates an ECC for given parameters.
type CreateECCFunc func(opts *Options) (encryption.Encryptor, error)

//nolint:gochecknoglobals
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

// Parameters encapsulates all ECC parameters.
type Parameters interface {
	GetECCAlgorithm() string
	GetECCOverheadPercent() int
}

// CreateEncryptor returns new encryption.Encryptor with error correction.
func CreateEncryptor(p Parameters) (encryption.Encryptor, error) {
	return CreateAlgorithm(&Options{
		Algorithm:       p.GetECCAlgorithm(),
		OverheadPercent: p.GetECCOverheadPercent(),
	})
}
