package content

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

func TestFormatters(t *testing.T) {
	secret := []byte("secret")

	data := make([]byte, 100)
	cryptorand.Read(data)
	h0 := sha1.Sum(data)

	for _, hashAlgo := range hashing.SupportedAlgorithms() {
		t.Run(hashAlgo, func(t *testing.T) {
			for _, encryptionAlgo := range encryption.SupportedAlgorithms(true) {
				t.Run(encryptionAlgo, func(t *testing.T) {
					ctx := testlogging.Context(t)

					fo := &format.ContentFormat{
						HMACSecret: secret,
						MasterKey:  make([]byte, 32),
						Hash:       hashAlgo,
						Encryption: encryptionAlgo,
					}

					hf, err := hashing.CreateHashFunc(fo)
					require.NoError(t, err)

					enc, err := encryption.CreateEncryptor(fo)
					require.NoError(t, err)

					contentID := hf(nil, gather.FromSlice(data))

					var cipherText gather.WriteBuffer
					defer cipherText.Close()

					require.NoError(t, enc.Encrypt(gather.FromSlice(data), contentID, &cipherText))

					var plainText gather.WriteBuffer
					defer plainText.Close()

					require.NoError(t, enc.Decrypt(cipherText.Bytes(), contentID, &plainText))

					h1 := sha1.Sum(plainText.ToByteSlice())
					assert.Equal(t, h0[:], h1[:], "Encrypt()/Decrypt() does not round-trip")

					verifyEndToEndFormatter(ctx, t, hashAlgo, encryptionAlgo)
				})
			}
		})
	}
}

//nolint:thelper
func verifyEndToEndFormatter(ctx context.Context, t *testing.T, hashAlgo, encryptionAlgo string) {
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	bm, err := NewManagerForTesting(testlogging.Context(t), st, mustCreateFormatProvider(t, &format.ContentFormat{
		Hash:       hashAlgo,
		Encryption: encryptionAlgo,
		HMACSecret: hmacSecret,
		MutableParameters: format.MutableParameters{
			Version:     1,
			MaxPackSize: maxPackSize,
		},
		MasterKey: make([]byte, 32), // zero key, does not matter
	}), nil, nil)
	require.NoErrorf(t, err, "can't create content manager with hash %v and encryption %v", hashAlgo, encryptionAlgo)

	defer bm.CloseShared(ctx)

	cases := []gather.Bytes{
		gather.FromSlice([]byte{}),
		gather.FromSlice([]byte{1, 2, 3}),
		gather.FromSlice(make([]byte, 256)),
		gather.FromSlice(bytes.Repeat([]byte{1, 2, 3, 5}, 1024)),
	}

	for _, b := range cases {
		contentID, err := bm.WriteContent(ctx, b, "", NoCompression)
		require.NoError(t, err)

		t.Logf("contentID %v", contentID)

		b2, err := bm.GetContent(ctx, contentID)

		require.NoErrorf(t, err, "unable to read content %q", contentID)
		require.Equalf(t, b.ToByteSlice(), b2, "content %q data mismatch", contentID)

		err = bm.Flush(ctx)
		require.NoError(t, err, "flush error")

		b3, err := bm.GetContent(ctx, contentID)

		require.NoErrorf(t, err, "unable to read content after flush %q", contentID)
		require.Equalf(t, b.ToByteSlice(), b3, "content %q data mismatch", contentID)
	}
}

func mustCreateFormatProvider(t *testing.T, f *format.ContentFormat) format.Provider {
	t.Helper()

	fop, err := format.NewFormattingOptionsProvider(f, nil)
	require.NoError(t, err)

	return fop
}
