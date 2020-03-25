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
	rand.Read(data) //nolint:errcheck

	masterKey := make([]byte, 32)
	rand.Read(masterKey) //nolint:errcheck

	contentID1 := make([]byte, 16)
	rand.Read(contentID1) //nolint:errcheck

	contentID2 := make([]byte, 16)
	rand.Read(contentID2) //nolint:errcheck

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

			if !e.IsDeprecated() && encryptionAlgo != encryption.DeprecatedNoneAlgorithm {
				cipherText1b, err2 := e.Encrypt(nil, data, contentID1)
				if err2 != nil || cipherText1b == nil {
					t.Errorf("invalid response from Encrypt: %v %v", cipherText1, err2)
				}

				if bytes.Equal(cipherText1, cipherText1b) {
					t.Errorf("multiple Encrypt returned the same ciphertext: %x", cipherText1)
				}
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

			if encryptionAlgo != encryption.DeprecatedNoneAlgorithm {
				if bytes.Equal(cipherText1, cipherText2) {
					t.Errorf("ciphertexts should be different, were %x", cipherText1)
				}

				// decrypt using wrong content ID
				badPlainText2, err := e.Decrypt(nil, cipherText2, contentID1)
				if e.IsAuthenticated() {
					if err == nil && encryptionAlgo != "SALSA20-HMAC" {
						// "SALSA20-HMAC" is deprecated & wrong, and only validates that checksum is
						// valid for some content, but does not validate that we decrypted the
						// intended content.
						t.Errorf("expected decrypt to fail for authenticated encryption")
					}
				} else {
					if bytes.Equal(badPlainText2, plainText2) {
						t.Errorf("decrypted plaintext matches, but it should not: %x", plainText2)
					}
				}

				// flip some bits in the cipherText
				if e.IsAuthenticated() {
					cipherText2[mathrand.Intn(len(cipherText2))] ^= byte(1 + mathrand.Intn(254))
					if _, err := e.Decrypt(nil, cipherText2, contentID1); err == nil {
						t.Errorf("expected decrypt failure on invalid ciphertext, got success")
					}
				}
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
				"NONE": hex.EncodeToString([]byte("foo")),

				"AES256-GCM-HMAC-SHA256":        "e43ba07f85a6d70c5f1102ca06cf19c597e5f91e527b21f00fb76e8bec3fd1",
				"CHACHA20-POLY1305-HMAC-SHA256": "118359f3d4d589d939efbbc3168ae4c77c51bcebce6845fe6ef5d11342faa6",

				// deprecated
				"AES-128-CTR":  "54cd8d",
				"AES-192-CTR":  "2d084b",
				"AES-256-CTR":  "8a580a",
				"SALSA20":      "bf5ec3",
				"SALSA20-HMAC": "8bf37fd9ec69843c3c2ac2a2cfdd59f36077206a15289efde640d0e677d03e6ac8f8ec",
			},
		},
		{
			masterKey: []byte("01234567890123456789012345678901"), // 32 bytes
			contentID: []byte("00000000000000000000000000000000"), // 32 bytes
			payload:   []byte("quick brown fox jumps over the lazy dog"),

			// samples of base16-encoded ciphertexts of payload encrypted with masterKey & contentID
			samples: map[string]string{
				"NONE": hex.EncodeToString([]byte("quick brown fox jumps over the lazy dog")),

				"AES256-GCM-HMAC-SHA256":        "eaad755a238f1daa4052db2e5ccddd934790b6cca415b3ccfd46ac5746af33d9d30f4400ffa9eb3a64fb1ce21b888c12c043bf6787d4a5c15ad10f21f6a6027ee3afe0",
				"CHACHA20-POLY1305-HMAC-SHA256": "836d2ba87892711077adbdbe1452d3b2c590bbfdf6fd3387dc6810220a32ec19de862e1a4f865575e328424b5f178afac1b7eeff11494f719d119b7ebb924d1d0846a3",

				// deprecated
				"AES-128-CTR":  "974c5c1782076e3de7255deabe8706a509b5772a8b7a8e7f83d01de7098c945934417071ec5351",
				"AES-192-CTR":  "1200e755ec14125e87136b5281957895eeb429be673b2241da261f949283aea59fd2fa64387764",
				"AES-256-CTR":  "39f13367828efb5fb22b97865ca0dbaad352d0c1a3083ff056bc771b812239445ed8af022f3760",
				"SALSA20":      "65ce12b14739aecbf9e6a9b9b9c4a72ffa8886fe0b071c0abdfb3d3e5c336b90f9af411ba69faf",
				"SALSA20-HMAC": "a1dc47f250def4d97a422d505fb5e9a9a13699762cb32cfe7705982fa68ce71f54544ab932a1045fb0601087159954d563f0de0aaa15690d93ea63748bf91889e577daeeed5cf8",
			},
		}}

	for _, tc := range cases {
		verifyCiphertextSamples(t, tc.masterKey, tc.contentID, tc.payload, tc.samples)
	}
}

func verifyCiphertextSamples(t *testing.T, masterKey, contentID, payload []byte, samples map[string]string) {
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
