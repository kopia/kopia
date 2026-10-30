// Package crypto implements common symmetric-encryption and key-derivation functions.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/pkg/errors"
)

//nolint:gochecknoglobals
const (
	purposeAESKey   = "AES"
	purposeAuthData = "CHECKSUM"
)

func initCrypto(masterKey, salt []byte) (cipher.AEAD, []byte, error) {
	aesKey, err := DeriveKeyFromMasterKey(masterKey, salt, purposeAESKey, 32) //nolint:mnd
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot derive AES key")
	}

	authData, err := DeriveKeyFromMasterKey(masterKey, salt, purposeAuthData, 32) //nolint:mnd
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot derive auth data")
	}

	blk, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create cipher")
	}

	aead, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create cipher")
	}

	return aead, authData, nil
}

var errPlaintextTooLarge = errors.New("plaintext data is too large to be encrypted")

// EncryptAes256Gcm encrypts data with AES 256 GCM.
func EncryptAes256Gcm(data, masterKey, salt []byte) ([]byte, error) {
	aead, authData, err := initCrypto(masterKey, salt)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialize crypto")
	}

	nonceLength := aead.NonceSize()
	noncePlusOverhead := nonceLength + aead.Overhead()

	const maxInt = int(^uint(0) >> 1)
	if len(data) > maxInt-noncePlusOverhead {
		return nil, errPlaintextTooLarge
	}

	cipherText := make([]byte, len(data)+noncePlusOverhead)

	// Store nonce at the beginning of ciphertext.
	nonce := cipherText[0:nonceLength]
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, errors.Wrap(err, "error reading random bytes for nonce")
	}

	b := aead.Seal(cipherText[nonceLength:nonceLength], nonce, data, authData)
	data = nonce[0 : nonceLength+len(b)]

	return data, nil
}

// DecryptAes256Gcm encrypts data with AES 256 GCM.
func DecryptAes256Gcm(data, masterKey, salt []byte) ([]byte, error) {
	aead, authData, err := initCrypto(masterKey, salt)
	if err != nil {
		return nil, errors.Wrap(err, "cannot initialize cipher")
	}

	if len(data) < aead.NonceSize() {
		return nil, errors.New("invalid encrypted payload, too short")
	}

	data = append([]byte(nil), data...)

	nonce := data[0:aead.NonceSize()]
	payload := data[aead.NonceSize():]

	plainText, err := aead.Open(payload[:0], nonce, payload, authData)
	if err != nil {
		return nil, errors.New("unable to decrypt repository blob, invalid credentials?")
	}

	return plainText, nil
}
