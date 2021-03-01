package cache

import (
	"crypto/sha256"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/encryption"
)

// encryptionProtectionAlgorithm is the authenticated encryption algorithm used by authenticatedEncryptionProtection.
var encryptionProtectionAlgorithm = "AES256-GCM-HMAC-SHA256"

// StorageProtection encapsulates protection (HMAC and/or encryption) applied to local cache items.
type StorageProtection interface {
	SupportsPartial() bool
	Protect(id string, b []byte) []byte
	Verify(id string, b []byte) ([]byte, error)
}

type nullStorageProtection struct{}

func (nullStorageProtection) Protect(id string, b []byte) []byte {
	return b
}

func (nullStorageProtection) Verify(id string, b []byte) ([]byte, error) {
	return b, nil
}

func (nullStorageProtection) SupportsPartial() bool {
	return true
}

// NoProtection returns implementation of StorageProtection that offers no protection.
func NoProtection() StorageProtection {
	return nullStorageProtection{}
}

type checksumProtection struct {
	Secret []byte
}

func (p checksumProtection) Protect(id string, b []byte) []byte {
	return hmac.Append(b, p.Secret)
}

func (p checksumProtection) Verify(id string, b []byte) ([]byte, error) {
	return hmac.VerifyAndStrip(b, p.Secret)
}

func (checksumProtection) SupportsPartial() bool {
	return false
}

// ChecksumProtection returns StorageProtection that protects cached data using HMAC checksums without encryption.
func ChecksumProtection(key []byte) StorageProtection {
	return checksumProtection{key}
}

type authenticatedEncryptionProtection struct {
	e encryption.Encryptor
}

func (p authenticatedEncryptionProtection) deriveIV(id string) []byte {
	contentID := sha256.Sum256([]byte(id))
	return contentID[:]
}

func (p authenticatedEncryptionProtection) Protect(id string, b []byte) []byte {
	c, err := p.e.Encrypt(nil, b, p.deriveIV(id))
	if err != nil {
		panic("encryption unexpectedly failed: " + err.Error())
	}

	return c
}

func (authenticatedEncryptionProtection) SupportsPartial() bool {
	return false
}

func (p authenticatedEncryptionProtection) Verify(id string, b []byte) ([]byte, error) {
	return p.e.Decrypt(nil, b, p.deriveIV(id))
}

type authenticatedEncryptionProtectionKey []byte

func (k authenticatedEncryptionProtectionKey) GetEncryptionAlgorithm() string {
	return encryptionProtectionAlgorithm
}

func (k authenticatedEncryptionProtectionKey) GetMasterKey() []byte {
	return k
}

// AuthenticatedEncryptionProtection returns StorageProtection that protects cached data using authenticated encryption.
func AuthenticatedEncryptionProtection(key []byte) (StorageProtection, error) {
	e, err := encryption.CreateEncryptor(authenticatedEncryptionProtectionKey(key))
	if err != nil {
		return nil, errors.Wrap(err, "unable to create encryptor")
	}

	if !e.IsAuthenticated() {
		return nil, errors.Wrap(err, "encryption is not authenticated!")
	}

	return authenticatedEncryptionProtection{e}, nil
}
