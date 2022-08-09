package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

const aes256GCMHmacSha256Overhead = 28

const aes256KeyDerivationSecretSize = 32

type aes256GCMHmacSha256 struct {
	hmacPool *sync.Pool
}

// aeadForContent returns cipher.AEAD using key derived from a given contentID.
func (e aes256GCMHmacSha256) aeadForContent(contentID []byte) (cipher.AEAD, error) {
	//nolint:forcetypeassert
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

	//nolint:wrapcheck
	return cipher.NewGCM(c)
}

func (e aes256GCMHmacSha256) Decrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return err
	}

	return aeadOpenPrefixedWithNonce(a, input, contentID, output)
}

func (e aes256GCMHmacSha256) Encrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return err
	}

	return aeadSealWithRandomNonce(a, input, contentID, output)
}

func (e aes256GCMHmacSha256) Overhead() int {
	return aes256GCMHmacSha256Overhead
}

func init() {
	Register("AES256-GCM-HMAC-SHA256", "AES-256-GCM using per-content key generated using HMAC-SHA256", false, func(p Parameters) (Encryptor, error) {
		keyDerivationSecret, err := deriveKey(p, []byte(purposeEncryptionKey), aes256KeyDerivationSecretSize)
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
