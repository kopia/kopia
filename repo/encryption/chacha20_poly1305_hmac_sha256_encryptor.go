package encryption

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/kopia/kopia/internal/gather"
)

const chacha20poly1305hmacSha256EncryptorOverhead = 28

const chacha20KeyDerivationSecretSize = 32

type chacha20poly1305hmacSha256Encryptor struct {
	hmacPool *sync.Pool
}

// aeadForContent returns cipher.AEAD using key derived from a given contentID.
func (e chacha20poly1305hmacSha256Encryptor) aeadForContent(contentID []byte) (cipher.AEAD, error) {
	//nolint:forcetypeassert
	h := e.hmacPool.Get().(hash.Hash)
	defer e.hmacPool.Put(h)

	h.Reset()

	if _, err := h.Write(contentID); err != nil {
		return nil, errors.Wrap(err, "unable to derive encryption key")
	}

	var hashBuf [32]byte
	key := h.Sum(hashBuf[:0])

	//nolint:wrapcheck
	return chacha20poly1305.New(key)
}

func (e chacha20poly1305hmacSha256Encryptor) Decrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return err
	}

	return aeadOpenPrefixedWithNonce(a, input, contentID, output)
}

func (e chacha20poly1305hmacSha256Encryptor) Encrypt(input gather.Bytes, contentID []byte, output *gather.WriteBuffer) error {
	a, err := e.aeadForContent(contentID)
	if err != nil {
		return err
	}

	return aeadSealWithRandomNonce(a, input, contentID, output)
}

func (e chacha20poly1305hmacSha256Encryptor) Overhead() int {
	return chacha20poly1305hmacSha256EncryptorOverhead
}

func init() {
	Register("CHACHA20-POLY1305-HMAC-SHA256", "CHACHA20-POLY1305 using per-content key generated using HMAC-SHA256", false, func(p Parameters) (Encryptor, error) {
		keyDerivationSecret, err := deriveKey(p, []byte(purposeEncryptionKey), chacha20KeyDerivationSecretSize)
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
