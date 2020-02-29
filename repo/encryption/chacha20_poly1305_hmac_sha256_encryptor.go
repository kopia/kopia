package encryption

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"

	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20poly1305"
)

type chacha20poly1305hmacSha256Encryptor struct {
	keyDerivationSecret []byte
}

// aeadForContent returns cipher.AEAD using key derived from a given contentID.
func (e chacha20poly1305hmacSha256Encryptor) aeadForContent(contentID []byte) (cipher.AEAD, error) {
	h := hmac.New(sha256.New, e.keyDerivationSecret)
	if _, err := h.Write(contentID); err != nil {
		return nil, errors.Wrap(err, "unable to derive encryption key")
	}

	key := h.Sum(nil)

	return chacha20poly1305.New(key)
}

func (e chacha20poly1305hmacSha256Encryptor) Decrypt(input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadOpenPrefixedWithNonce(a, input, contentID)
}

func (e chacha20poly1305hmacSha256Encryptor) Encrypt(input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadSealWithRandomNonce(a, input, contentID)
}

func (e chacha20poly1305hmacSha256Encryptor) IsAuthenticated() bool {
	return true
}

func (e chacha20poly1305hmacSha256Encryptor) IsDeprecated() bool {
	return false
}

func init() {
	Register("CHACHA20-POLY1305-HMAC-SHA256", "CHACHA20-POLY1305 using per-content key generated using HMAC-SHA256", false, func(p Parameters) (Encryptor, error) {
		keyDerivationSecret, err := deriveKey(p, []byte(purposeEncryptionKey), 32)
		if err != nil {
			return nil, err
		}

		return chacha20poly1305hmacSha256Encryptor{keyDerivationSecret}, nil
	})
}
