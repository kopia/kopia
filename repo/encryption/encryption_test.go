package encryption_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	mathrand "math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/encryption"
)

type parameters struct {
	encryptionAlgo string
	masterKey      []byte
}

func (p parameters) GetEncryptionAlgorithm() string { return p.encryptionAlgo }
func (p parameters) GetMasterKey() []byte           { return p.masterKey }

//nolint:gocyclo
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
		t.Run(encryptionAlgo, func(t *testing.T) {
			e, err := encryption.CreateEncryptor(parameters{encryptionAlgo, masterKey})
			if err != nil {
				t.Fatal(err)
			}

			var cipherText1 gather.WriteBuffer
			defer cipherText1.Close()

			var cipherText1b gather.WriteBuffer
			defer cipherText1b.Close()

			require.NoError(t, e.Encrypt(gather.FromSlice(data), contentID1, &cipherText1))
			require.NoError(t, e.Encrypt(gather.FromSlice(data), contentID1, &cipherText1b))

			if v := cipherText1.ToByteSlice(); bytes.Equal(v, cipherText1b.ToByteSlice()) {
				t.Errorf("multiple Encrypt returned the same ciphertext: %x", v)
			}

			var plainText1 gather.WriteBuffer
			defer plainText1.Close()

			require.NoError(t, e.Decrypt(cipherText1.Bytes(), contentID1, &plainText1))

			if v := plainText1.ToByteSlice(); !bytes.Equal(v, data) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", v, data)
			}

			var cipherText2 gather.WriteBuffer
			defer cipherText2.Close()

			require.NoError(t, e.Encrypt(gather.FromSlice(data), contentID2, &cipherText2))

			var plainText2 gather.WriteBuffer
			defer plainText2.Close()

			require.NoError(t, e.Decrypt(cipherText2.Bytes(), contentID2, &plainText2))

			if v := plainText2.ToByteSlice(); !bytes.Equal(v, data) {
				t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", v, data)
			}

			if v := cipherText1.ToByteSlice(); bytes.Equal(v, cipherText2.ToByteSlice()) {
				t.Errorf("ciphertexts should be different, were %x", v)
			}

			// decrypt using wrong content ID
			require.Error(t, e.Decrypt(cipherText2.Bytes(), contentID1, &plainText2))

			// flip some bits in the cipherText
			b := cipherText2.Bytes()
			b.Slices[0][mathrand.Intn(b.Length())] ^= byte(1 + mathrand.Intn(254))

			require.Error(t, e.Decrypt(b, contentID1, &plainText2))
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
			func() {
				var v gather.WriteBuffer
				defer v.Close()
				require.NoError(t, enc.Encrypt(gather.FromSlice(payload), contentID, &v))

				t.Errorf("missing ciphertext sample for %q: %q,", encryptionAlgo, hex.EncodeToString(payload))
			}()
		} else {
			b, err := hex.DecodeString(ct)
			if err != nil {
				t.Errorf("invalid ciphertext for %v: %v", encryptionAlgo, err)
				continue
			}

			func() {
				var plainText gather.WriteBuffer
				defer plainText.Close()

				require.NoError(t, enc.Decrypt(gather.FromSlice(b), contentID, &plainText))

				if v := plainText.ToByteSlice(); !bytes.Equal(v, payload) {
					t.Errorf("invalid plaintext after decryption %x, want %x", v, payload)
				}
			}()
		}
	}
}

func BenchmarkEncryption(b *testing.B) {
	masterKey := make([]byte, 32)
	rand.Read(masterKey)

	enc, err := encryption.CreateEncryptor(parameters{encryption.DefaultAlgorithm, masterKey})
	require.NoError(b, err)

	// 8 MiB
	plainText := gather.FromSlice(bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, 1<<20))

	var warmupOut gather.WriteBuffer

	iv := []byte{0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7}

	require.NoError(b, enc.Encrypt(plainText, iv, &warmupOut))
	warmupOut.Close()

	b.ResetTimer()

	for range b.N {
		var out gather.WriteBuffer

		enc.Encrypt(plainText, iv, &out)
		out.Close()
	}
}
