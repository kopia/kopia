package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
)

var (
	hkdfInfoBlockID   = []byte("BlockID")
	hkdfInfoCryptoKey = []byte("CryptoKey")
)

// Format describes the format of object data.
type Format struct {
	Version           string `json:"version"`
	ObjectFormat      string `json:"objectFormat"`
	Secret            []byte `json:"secret,omitempty"`
	MaxInlineBlobSize int    `json:"maxInlineBlobSize"`
	MaxBlobSize       int    `json:"maxBlobSize"`
}

// ObjectIDFormat describes single format ObjectID
type ObjectIDFormat struct {
	Name        string
	IsEncrypted bool

	hashBuffer   func(data []byte, secret []byte) ([]byte, []byte)
	createCipher func(key []byte) (cipher.Block, error)
}

// ObjectIDFormats is a collection of ObjectIDFormat
type ObjectIDFormats []*ObjectIDFormat

// Find returns the ObjectIDFormat with a given name or nil if not found.
func (fmts ObjectIDFormats) Find(name string) *ObjectIDFormat {
	for _, f := range fmts {
		if f.Name == name {
			return f
		}
	}

	return nil
}

func fold(b []byte, size int) []byte {
	if len(b) == size {
		return b
	}

	for i := size; i < len(b); i++ {
		b[i%size] ^= b[i]
	}
	return b[0:size]
}

func nonEncryptedFormat(name string, hf func() hash.Hash, hashSize int) *ObjectIDFormat {
	return &ObjectIDFormat{
		Name: name,
		hashBuffer: func(data []byte, secret []byte) ([]byte, []byte) {
			return fold(hashContent(hf, data, secret), hashSize), nil
		},
	}
}

func hashContent(hf func() hash.Hash, data []byte, secret []byte) []byte {
	var h hash.Hash

	if secret != nil {
		h = hmac.New(hf, secret)
	} else {
		h = hf()
	}
	h.Write(data)
	return h.Sum(nil)
}

func encryptedFormat(
	name string,
	hf func() hash.Hash,
	blockIDSize int,
	createCipher func(key []byte) (cipher.Block, error),
	keySize int,
) *ObjectIDFormat {
	return &ObjectIDFormat{
		Name: name,
		hashBuffer: func(data []byte, secret []byte) ([]byte, []byte) {
			contentHash := hashContent(sha512.New, data, secret)
			blockID := fold(hashContent(hf, hkdfInfoBlockID, contentHash), blockIDSize)
			cryptoKey := fold(hashContent(hf, hkdfInfoCryptoKey, contentHash), keySize)
			return blockID, cryptoKey
		},
		createCipher: createCipher,
	}
}

func buildObjectIDFormats() ObjectIDFormats {
	var result ObjectIDFormats

	for _, h := range []struct {
		name     string
		hash     func() hash.Hash
		hashSize int
	}{
		{"md5", md5.New, md5.Size},
		{"sha1", sha1.New, sha1.Size},
		{"sha224", sha256.New224, sha256.Size224},
		{"sha256", sha256.New, sha256.Size},
		{"sha256-fold128", sha256.New, 16},
		{"sha256-fold160", sha256.New, 20},
		{"sha384", sha512.New384, sha512.Size384},
		{"sha512-fold128", sha512.New, 16},
		{"sha512-fold160", sha512.New, 20},
		{"sha512-224", sha512.New512_224, sha512.Size224},
		{"sha512-256", sha512.New512_256, sha512.Size256},
		{"sha512", sha512.New, sha512.Size},
	} {
		result = append(result, nonEncryptedFormat(h.name, h.hash, h.hashSize))
		result = append(result, encryptedFormat(h.name+"-aes128", h.hash, h.hashSize, aes.NewCipher, 16))
		result = append(result, encryptedFormat(h.name+"-aes192", h.hash, h.hashSize, aes.NewCipher, 24))
		result = append(result, encryptedFormat(h.name+"-aes256", h.hash, h.hashSize, aes.NewCipher, 32))
	}

	return result
}

// SupportedFormats contains supported repository formats.
var SupportedFormats = buildObjectIDFormats()
