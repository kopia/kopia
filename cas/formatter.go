package cas

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io"

	"github.com/kopia/kopia/content"
)

type streamTransformer func(io.ReadCloser) io.ReadCloser

type objectFormatter interface {
	Do(b []byte, prefix string) (content.ObjectID, streamTransformer)
}

type nonEncryptingFormatter struct {
	hash func() hash.Hash
}

func (f *nonEncryptingFormatter) Do(b []byte, prefix string) (content.ObjectID, streamTransformer) {
	h := f.hash()
	h.Write(b)
	blockID := hex.EncodeToString(h.Sum(nil))

	return content.ObjectID(prefix + blockID), func(r io.ReadCloser) io.ReadCloser { return r }
}

func newNonEncryptingFormatter(hash func() hash.Hash) objectFormatter {
	return &nonEncryptingFormatter{
		hash: hash,
	}
}

type aesEncryptingFormatter struct {
	masterContentSecret []byte
}

func (f *aesEncryptingFormatter) Do(b []byte, prefix string) (content.ObjectID, streamTransformer) {
	// Compute HMAC-SHA512 of the content
	s := hmac.New(sha512.New, f.masterContentSecret)
	s.Write(b)
	contentHash := s.Sum(nil)

	// Split the hash into two portions - encryption key and content ID.
	aesKey := contentHash[0:32]
	return content.ObjectID(prefix + hex.EncodeToString(contentHash[32:64]) + ".e"),
		func(r io.ReadCloser) io.ReadCloser {
			var iv [aes.BlockSize]byte
			rand.Read(iv[:])

			validationKey := []byte{1, 2, 3, 4}

			aes, err := aes.NewCipher(aesKey)
			if err != nil {
				panic("")
			}

			ctr := cipher.NewCTR(aes, iv[:])

			return newEncryptingReader(r, iv[:], ctr, hmac.New(sha256.New, validationKey))
		}
}
