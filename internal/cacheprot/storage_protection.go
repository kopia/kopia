// Package cacheprot provides utilities for protection of cache entries.
package cacheprot

import (
	"crypto/sha256"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/repo/encryption"
)

// encryptionProtectionAlgorithm is the authenticated encryption algorithm used by authenticatedEncryptionProtection.
const encryptionProtectionAlgorithm = "AES256-GCM-HMAC-SHA256"

// StorageProtection encapsulates protection (HMAC and/or encryption) applied to local cache items.
type StorageProtection interface {
	Protect(id string, input gather.Bytes, output *gather.WriteBuffer)
	Verify(id string, input gather.Bytes, output *gather.WriteBuffer) error
}

type nullStorageProtection struct{}

func (nullStorageProtection) Protect(id string, input gather.Bytes, output *gather.WriteBuffer) {
	output.Reset()
	input.WriteTo(output) //nolint:errcheck
}

func (nullStorageProtection) Verify(id string, input gather.Bytes, output *gather.WriteBuffer) error {
	output.Reset()
	input.WriteTo(output) //nolint:errcheck

	return nil
}

// NoProtection returns implementation of StorageProtection that offers no protection.
func NoProtection() StorageProtection {
	return nullStorageProtection{}
}

type checksumProtection struct {
	Secret []byte
}

func (p checksumProtection) Protect(id string, input gather.Bytes, output *gather.WriteBuffer) {
	output.Reset()
	hmac.Append(input, p.Secret, output)
}

func (p checksumProtection) Verify(id string, input gather.Bytes, output *gather.WriteBuffer) error {
	output.Reset()
	//nolint:wrapcheck
	return hmac.VerifyAndStrip(input, p.Secret, output)
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

func (p authenticatedEncryptionProtection) Protect(id string, input gather.Bytes, output *gather.WriteBuffer) {
	output.Reset()

	impossible.PanicOnError(p.e.Encrypt(input, p.deriveIV(id), output))
}

func (p authenticatedEncryptionProtection) Verify(id string, input gather.Bytes, output *gather.WriteBuffer) error {
	output.Reset()

	if err := p.e.Decrypt(input, p.deriveIV(id), output); err != nil {
		return errors.Wrap(err, "unable to decrypt cache content")
	}

	return nil
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
	impossible.PanicOnError(err)

	return authenticatedEncryptionProtection{e}, nil
}
