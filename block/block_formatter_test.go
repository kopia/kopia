package block

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"testing"
)

func TestFormatters(t *testing.T) {
	secret := []byte("secret")
	f := FormattingOptions{HMACSecret: secret, MasterKey: make([]byte, 32)}

	for k, v := range FormatterFactories {
		data := make([]byte, 100)
		rand.Read(data)

		h0 := sha1.Sum(data)

		of, err := v(f)
		if err != nil {
			t.Errorf("error creating object formatter for %v: %v", k, err)
			continue
		}

		t.Logf("testing %v", k)
		blockID := of.ComputeBlockID(data)
		cipherText, err := of.Encrypt(data, blockID)
		if err != nil || cipherText == nil {
			t.Errorf("invalid response from Encrypt: %v %v", cipherText, err)
		}

		plainText, err := of.Decrypt(cipherText, blockID)
		if err != nil || plainText == nil {
			t.Errorf("invalid response from Decrypt: %v %v", plainText, err)
		}

		h1 := sha1.Sum(plainText)

		if !bytes.Equal(h0[:], h1[:]) {
			t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", h0, h1)
		}

		if len(blockID)%16 != 0 {
			t.Errorf("block ID for %v not a multiple of 16: %v", k, blockID)
		}
	}
}
