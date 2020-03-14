package encryption

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20poly1305"
)

const chacha20poly1305hmacSha256EncryptorOverhead = 28

type chacha20poly1305hmacSha256Encryptor struct {
	hmacPool *sync.Pool
}

// aeadForContent returns cipher.AEAD using key derived from a given contentID.
func (e chacha20poly1305hmacSha256Encryptor) aeadForContent(contentID []byte) (cipher.AEAD, error) {
	h := e.hmacPool.Get().(hash.Hash)
	defer e.hmacPool.Put(h)

	h.Reset()

	if _, err := h.Write(contentID); err != nil {
		return nil, errors.Wrap(err, "unable to derive encryption key")
	}

	var hashBuf [32]byte
	key := h.Sum(hashBuf[:0])

	return chacha20poly1305.New(key)
}

func (e chacha20poly1305hmacSha256Encryptor) Decrypt(output, input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadOpenPrefixedWithNonce(output, a, input, contentID)
}

func (e chacha20poly1305hmacSha256Encryptor) Encrypt(output, input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadSealWithRandomNonce(output, a, input, contentID)
}

func (e chacha20poly1305hmacSha256Encryptor) IsAuthenticated() bool {
	return true
}

func (e chacha20poly1305hmacSha256Encryptor) IsDeprecated() bool {
	return false
}

func (e chacha20poly1305hmacSha256Encryptor) MaxOverhead() int {
	return chacha20poly1305hmacSha256EncryptorOverhead
}

func init() {
	Register("CHACHA20-POLY1305-HMAC-SHA256", "CHACHA20-POLY1305 using per-content key generated using HMAC-SHA256", false, func(p Parameters) (Encryptor, error) {
		keyDerivationSecret, err := deriveKey(p, []byte(purposeEncryptionKey), 32)
		if err != nil {
			return nil, err
		}

		hmacPool := &sync.Pool{
			New: func() interface{} {
				return hmac.New(sha256.New, keyDerivationSecret)
			},
		}

		return chacha20poly1305hmacSha256Encryptor{hmacPool}, nil
	})
}
