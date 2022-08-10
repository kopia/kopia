// Package encryption manages content encryption algorithms.
package encryption

import (
	"crypto/sha256"
	"io"
	"sort"

	"github.com/pkg/errors"
	"golang.org/x/crypto/hkdf"

	"github.com/kopia/kopia/repo/transform"
)

const (
	minDerivedKeyLength = 32

	purposeEncryptionKey = "encryption"
)

// Parameters encapsulates all encryption parameters.
type Parameters interface {
	GetEncryptionAlgorithm() string
	GetMasterKey() []byte
}

// CreateEncryptor creates an encryption transform for given parameters.
func CreateEncryptor(p Parameters) (transform.Transformer, error) {
	e := encryptors[p.GetEncryptionAlgorithm()]
	if e == nil {
		return nil, errors.Errorf("unknown encryption algorithm: %v", p.GetEncryptionAlgorithm())
	}

	return e.newEncryptor(p)
}

// EncryptorFactory creates new encryption transform for given parameters.
type EncryptorFactory func(p Parameters) (transform.Transformer, error)

// DefaultAlgorithm is the name of the default encryption algorithm.
const DefaultAlgorithm = "AES256-GCM-HMAC-SHA256"

// SupportedAlgorithms returns the names of the supported encryption
// methods.
func SupportedAlgorithms(includeDeprecated bool) []string {
	var result []string

	for k, e := range encryptors {
		if e.deprecated && !includeDeprecated {
			continue
		}

		result = append(result, k)
	}

	sort.Strings(result)

	return result
}

// Register registers new encryption algorithm.
func Register(name, description string, deprecated bool, newEncryptor EncryptorFactory) {
	encryptors[name] = &encryptorInfo{
		description,
		deprecated,
		newEncryptor,
	}
}

type encryptorInfo struct {
	description  string
	deprecated   bool
	newEncryptor EncryptorFactory
}

//nolint:gochecknoglobals
var encryptors = map[string]*encryptorInfo{}

// deriveKey uses HKDF to derive a key of a given length and a given purpose from parameters.
func deriveKey(p Parameters, purpose []byte, length int) ([]byte, error) {
	if length < minDerivedKeyLength {
		return nil, errors.Errorf("derived key must be at least 32 bytes, was %v", length)
	}

	key := make([]byte, length)
	k := hkdf.New(sha256.New, p.GetMasterKey(), purpose, nil)
	io.ReadFull(k, key) //nolint:errcheck

	return key, nil
}
