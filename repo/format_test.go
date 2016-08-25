package repo

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"

	"testing"
)

func TestObjectFormatters(t *testing.T) {
	secret := []byte("secret")

	for k, v := range SupportedFormats {
		data := make([]byte, 100)
		rand.Read(data)

		h0 := sha1.Sum(data)

		t.Logf("testing %v", k)
		blk, key := v.ComputeBlockIDAndKey(data, secret)
		if key != nil {
			cipherText, err := v.Encrypt(data, key)
			if err != nil || cipherText == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText, err)
			}

			plainText, err := v.Decrypt(cipherText, key)
			if err != nil || plainText == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText, err)
			}

			h1 := sha1.Sum(plainText)

			if !bytes.Equal(h0[:], h1[:]) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", h0, h1)
			}
		} else {
			cipher, err := v.Encrypt(data, key)
			if err == nil || cipher != nil {
				t.Errorf("expected failure, but got response from Encrypt: %v %v", cipher, err)
			}
			plain, err := v.Decrypt(data, key)
			if err == nil || cipher != nil {
				t.Errorf("expected failure, but got response from Decrypt: %v %v", plain, err)
			}
		}

		if len(blk)%16 != 0 {
			t.Errorf("block ID for %v not a multiple of 16: %v", k, len(blk))
		}
	}
}
