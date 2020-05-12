package content

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha1"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// combinations of hash and encryption that are not compatible.
var incompatibleAlgorithms = map[string]string{
	"BLAKE2B-256-128/XSALSA20":      "expected >=24 bytes, got 16",
	"BLAKE2S-128/XSALSA20":          "expected >=24 bytes, got 16",
	"HMAC-RIPEMD-160/XSALSA20":      "expected >=24 bytes, got 20",
	"HMAC-SHA256-128/XSALSA20":      "expected >=24 bytes, got 16",
	"BLAKE2B-256-128/XSALSA20-HMAC": "expected >=24 bytes, got 16",
	"BLAKE2S-128/XSALSA20-HMAC":     "expected >=24 bytes, got 16",
	"HMAC-RIPEMD-160/XSALSA20-HMAC": "expected >=24 bytes, got 20",
	"HMAC-SHA256-128/XSALSA20-HMAC": "expected >=24 bytes, got 16",
}

func TestFormatters(t *testing.T) {
	secret := []byte("secret")

	data := make([]byte, 100)
	cryptorand.Read(data) //nolint:errcheck
	h0 := sha1.Sum(data)

	for _, hashAlgo := range hashing.SupportedAlgorithms() {
		hashAlgo := hashAlgo
		t.Run(hashAlgo, func(t *testing.T) {
			for _, encryptionAlgo := range encryption.SupportedAlgorithms(true) {
				encryptionAlgo := encryptionAlgo
				t.Run(encryptionAlgo, func(t *testing.T) {
					ctx := testlogging.Context(t)

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
							return
						}

						if !strings.HasSuffix(err.Error(), errmsg) {
							t.Errorf("unexpected error message %v, wanted %v", err.Error(), errmsg)
							return
						}

						return
					}

					contentID := h(nil, data)

					cipherText, err := e.Encrypt(nil, data, contentID)
					if err != nil || cipherText == nil {
						t.Errorf("invalid response from Encrypt: %v %v", cipherText, err)
					}

					plainText, err := e.Decrypt(nil, cipherText, contentID)
					if err != nil || plainText == nil {
						t.Errorf("invalid response from Decrypt: %v %v", plainText, err)
					}

					h1 := sha1.Sum(plainText)

					if !bytes.Equal(h0[:], h1[:]) {
						t.Errorf("Encrypt()/Decrypt() does not round-trip: %x %x", h0, h1)
					}

					verifyEndToEndFormatter(ctx, t, hashAlgo, encryptionAlgo)
				})
			}
		})
	}
}

func verifyEndToEndFormatter(ctx context.Context, t *testing.T, hashAlgo, encryptionAlgo string) {
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	bm, err := newManagerWithOptions(testlogging.Context(t), st, &FormattingOptions{
		Hash:        hashAlgo,
		Encryption:  encryptionAlgo,
		HMACSecret:  hmacSecret,
		MaxPackSize: maxPackSize,
		MasterKey:   make([]byte, 32), // zero key, does not matter
		Version:     1,
	}, nil, time.Now, nil)
	if err != nil {
		t.Errorf("can't create content manager with hash %v and encryption %v: %v", hashAlgo, encryptionAlgo, err.Error())
		return
	}

	defer bm.Close(ctx)

	cases := [][]byte{
		{},
		{1, 2, 3},
		make([]byte, 256),
		bytes.Repeat([]byte{1, 2, 3, 5}, 1024),
	}

	for _, b := range cases {
		contentID, err := bm.WriteContent(ctx, b, "")
		if err != nil {
			t.Errorf("err: %v", err)
		}

		t.Logf("contentID %v", contentID)

		b2, err := bm.GetContent(ctx, contentID)
		if err != nil {
			t.Errorf("unable to read content %q: %v", contentID, err)
			return
		}

		if got, want := b2, b; !bytes.Equal(got, want) {
			t.Errorf("content %q data mismatch: got %x, wanted %x", contentID, got, want)
			return
		}

		if err = bm.Flush(ctx); err != nil {
			t.Errorf("flush error: %v", err)
		}

		b3, err := bm.GetContent(ctx, contentID)
		if err != nil {
			t.Errorf("unable to read content after flush %q: %v", contentID, err)
			return
		}

		if got, want := b3, b; !bytes.Equal(got, want) {
			t.Errorf("content %q data mismatch: got %x, wanted %x", contentID, got, want)
			return
		}
	}
}
