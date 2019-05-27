package block

import (
	"bytes"
	"crypto/sha1"
	"math/rand"
	"testing"
)

// combinations of hash and encryption that are not compatible.
var incompatibleAlgorithms = map[string]string{
	"BLAKE2B-256-128/XSALSA20": "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"BLAKE2S-128/XSALSA20":     "invalid encryptor: hash too short, expected >=24 bytes, got 16",
	"HMAC-RIPEMD-160/XSALSA20": "invalid encryptor: hash too short, expected >=24 bytes, got 20",
	"HMAC-SHA256-128/XSALSA20": "invalid encryptor: hash too short, expected >=24 bytes, got 16",
}

func TestFormatters(t *testing.T) {
	secret := []byte("secret")

	data := make([]byte, 100)
	rand.Read(data)
	h0 := sha1.Sum(data)

	for _, hashAlgo := range SupportedHashAlgorithms() {
		for _, encryptionAlgo := range SupportedEncryptionAlgorithms() {
			h, e, err := CreateHashAndEncryptor(FormattingOptions{
				HMACSecret: secret,
				MasterKey:  make([]byte, 32),
				Hash:       hashAlgo,
				Encryption: encryptionAlgo,
			})

			if err != nil {
				key := hashAlgo + "/" + encryptionAlgo
				errmsg := incompatibleAlgorithms[key]
				if err.Error() == errmsg {
					continue
				}
				t.Errorf("Algorithm %v not marked as incompatible and failed with %v", key, err)
				continue
			}

			blockID := h(data)
			cipherText, err := e.Encrypt(data, blockID)
			if err != nil || cipherText == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText, err)
			}

			plainText, err := e.Decrypt(cipherText, blockID)
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
