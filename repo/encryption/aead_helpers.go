package encryption

import (
	"crypto/cipher"
	"crypto/rand"

	"github.com/pkg/errors"
)

// aeadSealWithRandomNonce returns AEAD-sealed content prepended with random nonce.
func aeadSealWithRandomNonce(result []byte, a cipher.AEAD, plaintext, contentID []byte) ([]byte, error) {
	resultLen := len(plaintext) + a.NonceSize() + a.Overhead()

	if cap(result) < resultLen {
		// result slice too small, make a new one
		result = make([]byte, 0, resultLen)
	}

	result = result[0:a.NonceSize()]

	n, err := rand.Read(result)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize nonce")
	}

	if n != a.NonceSize() {
		return nil, errors.Errorf("did not read exactly %v bytes, got %v", a.NonceSize(), n)
	}

	return a.Seal(result, result[0:a.NonceSize()], plaintext, contentID), nil
}

// aeadOpenPrefixedWithNonce opens AEAD-protected content, assuming first bytes are the nonce.
func aeadOpenPrefixedWithNonce(output []byte, a cipher.AEAD, ciphertext, contentID []byte) ([]byte, error) {
	if len(ciphertext) < a.NonceSize() {
		return nil, errors.Errorf("ciphertext too short")
	}

	return a.Open(output[:0], ciphertext[0:a.NonceSize()], ciphertext[a.NonceSize():], contentID)
}
