package encryption

import (
	"crypto/cipher"
	"crypto/rand"

	"github.com/pkg/errors"
)

// aeadSealWithRandomNonce returns AEAD-sealed content prepended with random nonce.
func aeadSealWithRandomNonce(a cipher.AEAD, plaintext, contentID []byte) ([]byte, error) {
	// pre-allocate a slice with len()=size of a nonce, and cap() for the entire ciphertext
	result := make([]byte, a.NonceSize(), len(plaintext)+a.NonceSize()+a.Overhead())

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
func aeadOpenPrefixedWithNonce(a cipher.AEAD, ciphertext, contentID []byte) ([]byte, error) {
	if len(ciphertext) < a.NonceSize() {
		return nil, errors.Errorf("ciphertext too short")
	}

	return a.Open(nil, ciphertext[0:a.NonceSize()], ciphertext[a.NonceSize():], contentID)
}
