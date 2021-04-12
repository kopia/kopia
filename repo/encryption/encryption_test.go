package encryption_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	mathrand "math/rand"
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
	rand.Read(data)

	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	contentID1 := make([]byte, 16)
	rand.Read(contentID1)

	contentID2 := make([]byte, 16)
	rand.Read(contentID2)

	for _, encryptionAlgo := range encryption.SupportedAlgorithms(true) {
		encryptionAlgo := encryptionAlgo
		t.Run(encryptionAlgo, func(t *testing.T) {
			e, err := encryption.CreateEncryptor(parameters{encryptionAlgo, masterKey})
			if err != nil {
				t.Fatal(err)
			}

			cipherText1, err := e.Encrypt(nil, data, contentID1)
			if err != nil || cipherText1 == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText1, err)
			}

			cipherText1b, err2 := e.Encrypt(nil, data, contentID1)
			if err2 != nil || cipherText1b == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText1, err2)
			}

			if bytes.Equal(cipherText1, cipherText1b) {
				t.Errorf("multiple Encrypt returned the same ciphertext: %x", cipherText1)
			}

			plainText1, err := e.Decrypt(nil, cipherText1, contentID1)
			if err != nil || plainText1 == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText1, err)
			}

			if !bytes.Equal(plainText1, data) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", plainText1, data)
			}

			plaintextOutput := make([]byte, 0, 256)

			plainText1a, err := e.Decrypt(plaintextOutput, cipherText1, contentID1)
			if err != nil || plainText1 == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText1, err)
			}

			if !bytes.Equal(plainText1a, plaintextOutput[0:len(plainText1a)]) {
				t.Errorf("Decrypt() does not use output buffer")
			}

			cipherText2, err := e.Encrypt(nil, data, contentID2)
			if err != nil || cipherText2 == nil {
				t.Errorf("invalid response from Encrypt: %v %v", cipherText2, err)
			}

			plainText2, err := e.Decrypt(nil, cipherText2, contentID2)
			if err != nil || plainText2 == nil {
				t.Errorf("invalid response from Decrypt: %v %v", plainText2, err)
			}

			if !bytes.Equal(plainText2, data) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", plainText2, data)
			}

			if bytes.Equal(cipherText1, cipherText2) {
				t.Errorf("ciphertexts should be different, were %x", cipherText1)
			}

			// decrypt using wrong content ID
			if _, err := e.Decrypt(nil, cipherText2, contentID1); err == nil {
				t.Fatalf("expected decrypt to fail for authenticated encryption")
			}

			// flip some bits in the cipherText
			cipherText2[mathrand.Intn(len(cipherText2))] ^= byte(1 + mathrand.Intn(254))
			if _, err := e.Decrypt(nil, cipherText2, contentID1); err == nil {
				t.Errorf("expected decrypt failure on invalid ciphertext, got success")
			}
		})
	}
}

func TestCiphertextSamples(t *testing.T) {
	cases := []struct {
		masterKey []byte
		contentID []byte
		payload   []byte
		samples   map[string]string
	}{
		{
			masterKey: []byte("01234567890123456789012345678901"), // 32 bytes
			contentID: []byte("aabbccddeeffgghhiijjkkllmmnnoopp"), // 32 bytes
			payload:   []byte("foo"),

			// samples of base16-encoded ciphertexts of payload encrypted with masterKey & contentID
			samples: map[string]string{
				"AES256-GCM-HMAC-SHA256":        "e43ba07f85a6d70c5f1102ca06cf19c597e5f91e527b21f00fb76e8bec3fd1",
				"CHACHA20-POLY1305-HMAC-SHA256": "118359f3d4d589d939efbbc3168ae4c77c51bcebce6845fe6ef5d11342faa6",
			},
		},
		{
			masterKey: []byte("01234567890123456789012345678901"), // 32 bytes
			contentID: []byte("00000000000000000000000000000000"), // 32 bytes
			payload:   []byte("quick brown fox jumps over the lazy dog"),

			// samples of base16-encoded ciphertexts of payload encrypted with masterKey & contentID
			samples: map[string]string{
				"AES256-GCM-HMAC-SHA256":        "eaad755a238f1daa4052db2e5ccddd934790b6cca415b3ccfd46ac5746af33d9d30f4400ffa9eb3a64fb1ce21b888c12c043bf6787d4a5c15ad10f21f6a6027ee3afe0",
				"CHACHA20-POLY1305-HMAC-SHA256": "836d2ba87892711077adbdbe1452d3b2c590bbfdf6fd3387dc6810220a32ec19de862e1a4f865575e328424b5f178afac1b7eeff11494f719d119b7ebb924d1d0846a3",
			},
		},
	}

	for _, tc := range cases {
		verifyCiphertextSamples(t, tc.masterKey, tc.contentID, tc.payload, tc.samples)
	}
}

func verifyCiphertextSamples(t *testing.T, masterKey, contentID, payload []byte, samples map[string]string) {
	t.Helper()

	for _, encryptionAlgo := range encryption.SupportedAlgorithms(true) {
		enc, err := encryption.CreateEncryptor(parameters{encryptionAlgo, masterKey})
		if err != nil {
			t.Fatal(err)
		}

		ct := samples[encryptionAlgo]
		if ct == "" {
			v, err := enc.Encrypt(nil, payload, contentID)
			if err != nil {
				t.Fatal(err)
			}

			t.Errorf("missing ciphertext sample for %q: %q,", encryptionAlgo, hex.EncodeToString(v))
		} else {
			b, err := hex.DecodeString(ct)
			if err != nil {
				t.Errorf("invalid ciphertext for %v: %v", encryptionAlgo, err)
				continue
			}

			plainText, err := enc.Decrypt(nil, b, contentID)
			if err != nil {
				t.Errorf("unable to decrypt %v: %v", encryptionAlgo, err)
				continue
			}

			if !bytes.Equal(plainText, payload) {
				t.Errorf("invalid plaintext after decryption %x, want %x", plainText, payload)
			}
		}
	}
}
