package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
	"sync"

	"github.com/pkg/errors"
)

const aes256GCMHmacSha256Overhead = 28

type aes256GCMHmacSha256 struct {
	hmacPool *sync.Pool
}

// aeadForContent returns cipher.AEAD using key derived from a given contentID.
func (e aes256GCMHmacSha256) aeadForContent(contentID []byte) (cipher.AEAD, error) {
	h := e.hmacPool.Get().(hash.Hash)
	defer e.hmacPool.Put(h)
	h.Reset()

	if _, err := h.Write(contentID); err != nil {
		return nil, errors.Wrap(err, "unable to derive encryption key")
	}

	var hashBuf [32]byte
	key := h.Sum(hashBuf[:0])

	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create AES-256 cipher")
	}

	return cipher.NewGCM(c)
}

func (e aes256GCMHmacSha256) Decrypt(output, input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadOpenPrefixedWithNonce(output, a, input, contentID)
}

func (e aes256GCMHmacSha256) Encrypt(output, input, contentID []byte) ([]byte, error) {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return nil, err
	}

	return aeadSealWithRandomNonce(output, a, input, contentID)
}

func (e aes256GCMHmacSha256) IsAuthenticated() bool {
	return true
}

func (e aes256GCMHmacSha256) IsDeprecated() bool {
	return false
}

func (e aes256GCMHmacSha256) MaxOverhead() int {
	return aes256GCMHmacSha256Overhead
}

func init() {
	Register("AES256-GCM-HMAC-SHA256", "AES-256-GCM using per-content key generated using HMAC-SHA256", false, func(p Parameters) (Encryptor, error) {
		keyDerivationSecret, err := deriveKey(p, []byte(purposeEncryptionKey), 32)
		if err != nil {
			return nil, err
		}

		hmacPool := &sync.Pool{
			New: func() interface{} {
				return hmac.New(sha256.New, keyDerivationSecret)
			},
		}

		return aes256GCMHmacSha256{hmacPool}, nil
	})
}
