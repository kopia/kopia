package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"hash"
)

// Format describes the format of objects in a repository.
type Format struct {
	Version                int32  `json:"version,omitempty"`                // version number, must be "1"
	ObjectFormat           string `json:"objectFormat,omitempty"`           // identifier of object format
	Secret                 []byte `json:"secret,omitempty"`                 // HMAC secret used to generate encryption keys
	MaxInlineContentLength int32  `json:"maxInlineContentLength,omitempty"` // maximum size of object to be considered for inline storage within ObjectID
	MaxBlockSize           int32  `json:"maxBlockSize,omitempty"`           // maximum size of storage block
}

// ObjectFormatter performs data block ID computation and encryption of a block of data when storing object in a repository,
type ObjectFormatter interface {
	// ComputeBlockIDAndKey computes ID of the storage block and encryption key for the specified block of data.
	// The secret should be used for HMAC.
	ComputeBlockIDAndKey(data []byte, secret []byte) (blockID string, cryptoKey []byte)

	// Encrypt returns encrypted bytes corresponding to the given plaintext.
	// Encryption in-place is allowed within original slice's capacity.
	Encrypt(plainText []byte, key []byte) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext.
	// Decryption in-place is allowed within original slice's capacity.
	Decrypt(cipherText []byte, key []byte) ([]byte, error)
}

type unencryptedFormat struct {
	hashCtor func() hash.Hash
	fold     int
}

func (fi *unencryptedFormat) ComputeBlockIDAndKey(data []byte, secret []byte) (blockID string, cryptoKey []byte) {
	h := hashContent(fi.hashCtor, data, secret)
	if fi.fold > 0 {
		h = fold(h, fi.fold)
	}
	blockID = hex.EncodeToString(h)
	return
}

func (fi *unencryptedFormat) Encrypt(plainText []byte, key []byte) ([]byte, error) {
	return nil, errors.New("encryption not supported")
}

func (fi *unencryptedFormat) Decrypt(cipherText []byte, key []byte) ([]byte, error) {
	return nil, errors.New("decryption not supported")
}

// Since we never share keys, using constant IV is fine.
// Instead of using all-zero, we use this one.
var constantIV = []byte("kopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopia")

type encryptedFormat struct {
	hashCtor     func() hash.Hash
	createCipher func(key []byte) (cipher.Block, error)
	keyBytes     int
}

func (fi *encryptedFormat) ComputeBlockIDAndKey(data []byte, secret []byte) (blockID string, cryptoKey []byte) {
	h := hashContent(fi.hashCtor, data, secret)
	p := len(h) - fi.keyBytes
	blockID = hex.EncodeToString(h[0:p])
	cryptoKey = h[p:]
	return
}

func (fi *encryptedFormat) Encrypt(plainText []byte, key []byte) ([]byte, error) {
	blockCipher, err := fi.createCipher(key)
	if err != nil {
		return nil, err
	}

	return xorKeyStream(plainText, blockCipher), nil
}

func (fi *encryptedFormat) Decrypt(cipherText []byte, key []byte) ([]byte, error) {
	blockCipher, err := fi.createCipher(key)
	if err != nil {
		return nil, err
	}

	return xorKeyStream(cipherText, blockCipher), nil
}

func xorKeyStream(b []byte, blockCipher cipher.Block) []byte {
	ctr := cipher.NewCTR(blockCipher, constantIV[0:blockCipher.BlockSize()])
	ctr.XORKeyStream(b, b)
	return b
}

// SupportedFormats is a map with an ObjectFormatter for each supported object format:
//
//   UNENCRYPTED_HMAC_SHA256_128          - unencrypted, block IDs are 128-bit (32 characters long)
//   UNENCRYPTED_HMAC_SHA256              - unencrypted, block IDs are 256-bit (64 characters long)
//   ENCRYPTED_HMAC_SHA512_384_AES256     - encrypted with AES-256, block IDs are 128-bit (32 characters long)
//   ENCRYPTED_HMAC_SHA512_AES256         - encrypted with AES-256, block IDs are 256-bit (64 characters long)
//
// Additional formats can be supported by adding them to the map.
var SupportedFormats map[string]ObjectFormatter

func init() {
	SupportedFormats = map[string]ObjectFormatter{
		"TESTONLY_MD5":                     &unencryptedFormat{md5.New, 0},
		"UNENCRYPTED_HMAC_SHA256":          &unencryptedFormat{sha256.New, 0},
		"UNENCRYPTED_HMAC_SHA256_128":      &unencryptedFormat{sha256.New, 16},
		"ENCRYPTED_HMAC_SHA512_384_AES256": &encryptedFormat{sha512.New384, aes.NewCipher, 32},
		"ENCRYPTED_HMAC_SHA512_AES256":     &encryptedFormat{sha512.New, aes.NewCipher, 32},
	}
}

// DefaultObjectFormat is the format that should be used by default when creating new repositories.
var DefaultObjectFormat = "ENCRYPTED_HMAC_SHA512_384_AES256"

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
