package encryption_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/kopia/kopia/repo/encryption"
)

type parameters struct {
	encryptionAlgo string
	masterKey      []byte
}

func (p parameters) GetEncryptionAlgorithm() string { return p.encryptionAlgo }
func (p parameters) GetMasterKey() []byte           { return p.masterKey }

// nolint:gocyclo
func TestRoundTrip(t *testing.T) {
	data := make([]byte, 100)
	rand.Read(data) //nolint:errcheck

	masterKey := make([]byte, 32)
	rand.Read(masterKey) //nolint:errcheck

	contentID1 := make([]byte, 16)
	rand.Read(contentID1) //nolint:errcheck

	contentID2 := make([]byte, 16)
	rand.Read(contentID2) //nolint:errcheck

	for _, encryptionAlgo := range encryption.SupportedAlgorithms() {
		encryptionAlgo := encryptionAlgo
		t.Run(encryptionAlgo, func(t *testing.T) {
			e, err := encryption.CreateEncryptor(parameters{encryptionAlgo, masterKey})
			if err != nil {
				t.Fatal(err)
			}

			cipherText1, err := e.Encrypt(data, contentID1)
			if err != nil || cipherText1 == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText1, err)
			}

			plainText1, err := e.Decrypt(cipherText1, contentID1)
			if err != nil || plainText1 == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText1, err)
			}

			if !bytes.Equal(plainText1, data) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", plainText1, data)
			}

			cipherText2, err := e.Encrypt(data, contentID2)
			if err != nil || cipherText2 == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText2, err)
			}

			plainText2, err := e.Decrypt(cipherText2, contentID2)
			if err != nil || plainText2 == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText2, err)
			}

			if !bytes.Equal(plainText2, data) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", plainText2, data)
			}

			if encryptionAlgo != encryption.NoneAlgorithm {
				if bytes.Equal(cipherText1, cipherText2) {
					t.Errorf("ciphertexts should be different, were %x", cipherText1)
				}

				// decrypt using wrong content ID
				badPlainText2, err := e.Decrypt(cipherText2, contentID1)
				if err != nil || plainText2 == nil {
					t.Errorf("invalid response from Decrypt: %v %v", plainText2, err)
				}

				if bytes.Equal(badPlainText2, plainText2) {
					t.Errorf("decrypted plaintext matches, but it should not: %x", plainText2)
				}
			}
		})
	}
}
