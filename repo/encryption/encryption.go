// Package encryption manages content encryption algorithms.
package encryption

import (
	"crypto/sha256"
	"io"
	"sort"

	"github.com/pkg/errors"
	"golang.org/x/crypto/hkdf"
)

const minDerivedKeyLength = 32

// Encryptor performs encryption and decryption of contents of data.
type Encryptor interface {
	// Encrypt appends the encrypted bytes corresponding to the given plaintext to a given slice.
	// Must not clobber the input slice and return ciphertext with additional padding and checksum.
	Encrypt(output, plainText, contentID []byte) ([]byte, error)

	// Decrypt appends the unencrypted bytes corresponding to the given ciphertext to a given slice.
	// Must not clobber the input slice. If IsAuthenticated() == true, Decrypt will perform
	// authenticity check before decrypting.
	Decrypt(output, cipherText, contentID []byte) ([]byte, error)

	// IsAuthenticated returns true if encryption is authenticated.
	// In this case Decrypt() is expected to perform authenticity check.
	IsAuthenticated() bool

	// IsDeprecated returns true if encryption is not recommended for new repositories.
	IsDeprecated() bool

	// MaxOverhead is the maximum number of bytes of overhead added by Encrypt()
	MaxOverhead() int
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
const DefaultAlgorithm = "AES256-GCM-HMAC-SHA256"

// DeprecatedNoneAlgorithm is the name of the algorithm that does not encrypt.
const DeprecatedNoneAlgorithm = "NONE"

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

var encryptors = map[string]*encryptorInfo{}

// deriveKey uses HKDF to derive a key of a given length and a given purpose from parameters.
// nolint:unparam
func deriveKey(p Parameters, purpose []byte, length int) ([]byte, error) {
	if length < minDerivedKeyLength {
		return nil, errors.Errorf("derived key must be at least 32 bytes, was %v", length)
	}

	key := make([]byte, length)
	k := hkdf.New(sha256.New, p.GetMasterKey(), purpose, nil)
	io.ReadFull(k, key) //nolint:errcheck

	return key, nil
}

// sliceForAppend takes a slice and a requested number of bytes. It returns a
// slice with the contents of the given slice followed by that many bytes and a
// second slice that aliases into it and contains only the extra bytes. If the
// original slice has sufficient capacity then no allocation is performed.
//
// From: https://golang.org/src/crypto/cipher/gcm.go
// Copyright 2013 The Go Authors. All rights reserved.
func sliceForAppend(in []byte, n int) (head, tail []byte) {
	if total := len(in) + n; cap(in) >= total {
		head = in[:total]
	} else {
		head = make([]byte, total)
		copy(head, in)
	}

	tail = head[len(in):]

	return
}
