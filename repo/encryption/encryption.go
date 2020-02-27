// Package encryption manages content encryption algorithms.
package encryption

import (
	"crypto/sha256"
	"io"
	"sort"

	"github.com/pkg/errors"
	"golang.org/x/crypto/hkdf"
)

// Encryptor performs encryption and decryption of contents of data.
type Encryptor interface {
	// Encrypt returns encrypted bytes corresponding to the given plaintext.
	// Must not clobber the input slice and return ciphertext with additional padding and checksum.
	Encrypt(plainText, contentID []byte) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext.
	// Must not clobber the input slice. If IsAuthenticated() == true, Decrypt will perform
	// authenticity check before decrypting.
	Decrypt(cipherText, contentID []byte) ([]byte, error)

	// IsAuthenticated returns true if encryption is authenticated.
	// In this case Decrypt() is expected to perform authenticity check.
	IsAuthenticated() bool
}

// Parameters encapsulates all encryption parameters.
type Parameters interface {
	GetEncryptionAlgorithm() string
	GetMasterKey() []byte
}

// CreateEncryptor creates an Encryptor for given parameters.
func CreateEncryptor(p Parameters) (Encryptor, error) {
	e := encryptors[p.GetEncryptionAlgorithm()]
	if e == nil {
		return nil, errors.Errorf("unknown encryption algorithm: %v", p.GetEncryptionAlgorithm())
	}

	return e.newEncryptor(p)
}

// EncryptorFactory creates new Encryptor for given parameters.
type EncryptorFactory func(p Parameters) (Encryptor, error)

// DefaultAlgorithm is the name of the default encryption algorithm.
const DefaultAlgorithm = "SALSA20-HMAC"

// NoneAlgorithm is the name of the algorithm that does not encrypt.
const NoneAlgorithm = "NONE"

// SupportedAlgorithms returns the names of the supported encryption
// methods
func SupportedAlgorithms() []string {
	var result []string
	for k := range encryptors {
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

var encryptors = map[string]*encryptorInfo{}

func cloneBytes(b []byte) []byte {
	return append([]byte{}, b...)
}

// deriveKey uses HKDF to derive a key of a given length and a given purpose from parameters.
// nolint:unparam
func deriveKey(p Parameters, purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, p.GetMasterKey(), purpose, nil)
	io.ReadFull(k, key) //nolint:errcheck

	return key
}
