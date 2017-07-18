package repo

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"testing"

	"github.com/kopia/kopia/internal/config"
)

func TestObjectFormatters(t *testing.T) {
	secret := []byte("secret")
	f := &config.RepositoryObjectFormat{Secret: secret, MasterKey: make([]byte, 32)}

	for k, v := range objectFormatterFactories {
		data := make([]byte, 100)
		rand.Read(data)

		h0 := sha1.Sum(data)

		of, err := v(f)
		if err != nil {
			t.Errorf("error creating object formatter for %v: %v", k, err)
			continue
		}

		t.Logf("testing %v", k)
		oid := of.ComputeObjectID(data)
		cipherText, err := of.Encrypt(data, oid)
		if err != nil || cipherText == nil {
			t.Errorf("invalid response from Encrypt: %v %v", cipherText, err)
		}

		plainText, err := of.Decrypt(cipherText, oid)
		if err != nil || plainText == nil {
			t.Errorf("invalid response from Decrypt: %v %v", plainText, err)
		}

		h1 := sha1.Sum(plainText)

		if !bytes.Equal(h0[:], h1[:]) {
			t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", h0, h1)
		}

		if len(oid.StorageBlock)%16 != 0 {
			t.Errorf("block ID for %v not a multiple of 16: %v", k, oid.StorageBlock)
		}
	}
}
