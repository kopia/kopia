package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"

	"github.com/pkg/errors"
)

type aes256GCMHmacSha256 struct {
	keyDerivationSecret []byte
}

// aeadForContent returns cipher.AEAD using key derived from a given contentID.
func (e aes256GCMHmacSha256) aeadForContent(contentID []byte) (cipher.AEAD, error) {
	h := hmac.New(sha256.New, e.keyDerivationSecret)
	if _, err := h.Write(contentID); err != nil {
		return nil, errors.Wrap(err, "unable to derive encryption key")
	}

	key := h.Sum(nil)

	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create AES-256 cipher")
	}

	return cipher.NewGCM(c)
}

func (e aes256GCMHmacSha256) Decrypt(input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadOpenPrefixedWithNonce(a, input, contentID)
}

func (e aes256GCMHmacSha256) Encrypt(input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadSealWithRandomNonce(a, input, contentID)
}

func (e aes256GCMHmacSha256) IsAuthenticated() bool {
	return true
}

func (e aes256GCMHmacSha256) IsDeprecated() bool {
	return false
}

func init() {
	Register("AES256-GCM-HMAC-SHA256", "AES-256-GCM using per-content key generated using HMAC-SHA256", false, func(p Parameters) (Encryptor, error) {
		keyDerivationSecret, err := deriveKey(p, []byte(purposeEncryptionKey), 32)
		if err != nil {
			return nil, err
		}

		return aes256GCMHmacSha256{keyDerivationSecret}, nil
	})
}
