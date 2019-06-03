package content

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"testing"
)

// combinations of hash and encryption that are not compatible.
var incompatibleAlgorithms = map[string]string{
	"BLAKE2B-256-128/XSALSA20":      "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"BLAKE2S-128/XSALSA20":          "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"HMAC-RIPEMD-160/XSALSA20":      "invalid encryptor: hash too short, expected >=24 bytes, got 20",
	"HMAC-SHA256-128/XSALSA20":      "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"BLAKE2B-256-128/XSALSA20-HMAC": "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"BLAKE2S-128/XSALSA20-HMAC":     "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"HMAC-RIPEMD-160/XSALSA20-HMAC": "invalid encryptor: hash too short, expected >=24 bytes, got 20",
	"HMAC-SHA256-128/XSALSA20-HMAC": "invalid encryptor: hash too short, expected >=24 bytes, got 16",
}

func TestFormatters(t *testing.T) {
	secret := []byte("secret")

	data := make([]byte, 100)
	cryptorand.Read(data) //nolint:errcheck
	h0 := sha1.Sum(data)

	for _, hashAlgo := range SupportedHashAlgorithms() {
		for _, encryptionAlgo := range SupportedEncryptionAlgorithms() {
			h, e, err := CreateHashAndEncryptor(&FormattingOptions{
				HMACSecret: secret,
				MasterKey:  make([]byte, 32),
				Hash:       hashAlgo,
				Encryption: encryptionAlgo,
			})

			if err != nil {
				key := hashAlgo + "/" + encryptionAlgo
				errmsg := incompatibleAlgorithms[key]
				if errmsg == "" {
					t.Errorf("Algorithm %v not marked as incompatible and failed with %v", key, err)
					continue
				}
				if err.Error() == errmsg {
					t.Errorf("unexpected error message %v, wanted %v", err.Error(), errmsg)
					continue
				}
				continue
			}

			contentID := h(data)
			cipherText, err := e.Encrypt(data, contentID)
			if err != nil || cipherText == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText, err)
			}

			plainText, err := e.Decrypt(cipherText, contentID)
			if err != nil || plainText == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText, err)
			}

			h1 := sha1.Sum(plainText)

			if !bytes.Equal(h0[:], h1[:]) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", h0, h1)
			}
		}
	}
}
