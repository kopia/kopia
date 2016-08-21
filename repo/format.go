package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"hash"
)

// Format describes the format of object data.
type Format struct {
	Version           int32  `json:"version,omitempty"`
	ObjectFormat      string `json:"objectFormat,omitempty"`
	Secret            []byte `json:"secret,omitempty"`
	MaxInlineBlobSize int32  `json:"maxInlineBlobSize,omitempty"`
	MaxBlobSize       int32  `json:"maxBlobSize,omitempty"`
}

// ObjectIDFormatInfo performs hashing and encryption of a data block.
type ObjectIDFormatInfo interface {
	HashBuffer(data []byte, secret []byte) (blockID []byte, cryptoKey []byte)
	CreateCipher(key []byte) (cipher.Block, error)
}

type unencryptedFormat struct {
	hashCtor func() hash.Hash
	fold     int
}

func (fi *unencryptedFormat) HashBuffer(data []byte, secret []byte) (blockID []byte, cryptoKey []byte) {
	blockID = hashContent(fi.hashCtor, data, secret)
	if fi.fold > 0 {
		blockID = fold(blockID, fi.fold)
	}
	return
}

func (fi *unencryptedFormat) CreateCipher(key []byte) (cipher.Block, error) {
	return nil, errors.New("encryption not supported")

}

type encryptedFormat struct {
	hashCtor     func() hash.Hash
	createCipher func(key []byte) (cipher.Block, error)
	keyBytes     int
}

func (fi *encryptedFormat) HashBuffer(data []byte, secret []byte) (blockID []byte, cryptoKey []byte) {
	h := hashContent(fi.hashCtor, data, secret)
	p := len(h) - fi.keyBytes
	blockID = h[0:p]
	cryptoKey = h[p:]
	return
}

func (fi *encryptedFormat) CreateCipher(key []byte) (cipher.Block, error) {
	return fi.createCipher(key)

}

// SupportedFormats containes supported ObjectIDformats
var SupportedFormats = map[string]ObjectIDFormatInfo{
	"TESTONLY_MD5":                     &unencryptedFormat{md5.New, 0},
	"UNENCRYPTED_HMAC_SHA256":          &unencryptedFormat{sha256.New, 0},
	"UNENCRYPTED_HMAC_SHA256_128":      &unencryptedFormat{sha256.New, 16},
	"ENCRYPTED_HMAC_SHA512_384_AES256": &encryptedFormat{sha512.New384, aes.NewCipher, 32},
	"ENCRYPTED_HMAC_SHA512_AES256":     &encryptedFormat{sha512.New, aes.NewCipher, 32},
}

// DefaultObjectFormat is the format that should be used by default when creating new repositories.
var DefaultObjectFormat = "ENCRYPTED_HMAC_SHA512_AES256"

func fold(b []byte, size int) []byte {
	if len(b) == size {
		return b
	}

	for i := size; i < len(b); i++ {
		b[i%size] ^= b[i]
	}
	return b[0:size]
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
