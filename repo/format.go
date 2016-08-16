package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
)

var (
	hkdfInfoBlockID   = []byte("BlockID")
	hkdfInfoCryptoKey = []byte("CryptoKey")
)

// ObjectIDFormatInfo performs hashing and encryption of a data block.
type ObjectIDFormatInfo interface {
	Name() string
	HashBuffer(data []byte, secret []byte) (blockID []byte, cryptoKey []byte)
	CreateCipher(key []byte) (cipher.Block, error)
}

type unencryptedFormat struct {
	name     string
	hashCtor func() hash.Hash
	fold     int
}

func (fi *unencryptedFormat) Name() string {
	return fi.name
}

func (fi *unencryptedFormat) HashBuffer(data []byte, secret []byte) (blockID []byte, cryptoKey []byte) {
	blockID = hashContent(fi.hashCtor, data, secret)
	if fi.fold > 0 {
		for i := fi.fold; i < len(blockID); i++ {
			blockID[i%fi.fold] ^= blockID[i]
		}
		blockID = blockID[0:fi.fold]
	}
	return
}

func (fi *unencryptedFormat) CreateCipher(key []byte) (cipher.Block, error) {
	return nil, errors.New("encryption not supported")

}

type encryptedFormat struct {
	name         string
	hashCtor     func() hash.Hash
	createCipher func(key []byte) (cipher.Block, error)
	keyBytes     int
}

func (fi *encryptedFormat) Name() string {
	return fi.name
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
var SupportedFormats = map[ObjectIDFormat]ObjectIDFormatInfo{
	ObjectIDFormat_TESTONLY_MD5:                     &unencryptedFormat{"TESTONLY_MD5", md5.New, 0},
	ObjectIDFormat_UNENCYPTED_HMAC_SHA256:           &unencryptedFormat{"UNENCRYPTED_HMAC_SHA256", sha256.New, 0},
	ObjectIDFormat_UNENCYPTED_HMAC_SHA256_128:       &unencryptedFormat{"UNENCRYPTED_HMAC_SHA256", sha256.New, 16},
	ObjectIDFormat_ENCRYPTED_HMAC_SHA512_384_AES256: &encryptedFormat{"ENCRYPTED_HMAC_SHA512_384_AES256", sha512.New384, aes.NewCipher, 32},
	ObjectIDFormat_ENCRYPTED_HMAC_SHA512_AES256:     &encryptedFormat{"ENCRYPTED_HMAC_SHA512_AES256", sha512.New, aes.NewCipher, 32},
}

// DefaultObjectFormat is the format that should be used by default when creating new repositories.
var DefaultObjectFormat = SupportedFormats[ObjectIDFormat_ENCRYPTED_HMAC_SHA512_AES256]

// ParseObjectIDFormat parses the given string and returns corresponding ObjectIDFormat.
func ParseObjectIDFormat(s string) (ObjectIDFormat, error) {
	v, ok := ObjectIDFormat_value[s]
	if ok {
		return ObjectIDFormat(v), nil
	}

	return ObjectIDFormat_INVALID, fmt.Errorf("unknown object ID format: %v", s)
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
