package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
)

// Format describes the format of objects in a repository.
type Format struct {
	Version                int32  `json:"version,omitempty"`                // version number, must be "1"
	ObjectFormat           string `json:"objectFormat,omitempty"`           // identifier of object format
	Secret                 []byte `json:"secret,omitempty"`                 // HMAC secret used to generate encryption keys
	MaxInlineContentLength int32  `json:"maxInlineContentLength,omitempty"` // maximum size of object to be considered for inline storage within ObjectID
	MasterKey              []byte `json:"masterKey,omitempty"`              // master encryption key (SIV-mode encryption only)
	Splitter               string `json:"splitter,omitempty"`               // splitter used to break objects into storage blocks

	MinBlockSize int32 `json:"minBlockSize,omitempty"` // minimum block size used with dynamic splitter
	AvgBlockSize int32 `json:"avgBlockSize,omitempty"` // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize int32 `json:"maxBlockSize,omitempty"` // maximum size of storage block
}

// Validate checks the validity of a Format and returns an error if invalid.
func (f *Format) Validate() error {
	if f.Version != 1 {
		return fmt.Errorf("unsupported version: %v", f.Version)
	}

	if f.MaxBlockSize < 100 {
		return fmt.Errorf("MaxBlockSize is not set")
	}

	sf := SupportedFormats[f.ObjectFormat]
	if sf == nil {
		return fmt.Errorf("unknown object format: %v", f.ObjectFormat)
	}

	return nil
}

// ObjectFormatter performs data block ID computation and encryption of a block of data when storing object in a repository,
type ObjectFormatter interface {
	// ComputeObjectID computes ID of the storage block and encryption key for the specified block of data
	// and returns them in ObjectID.
	ComputeObjectID(data []byte) ObjectID

	// Encrypt returns encrypted bytes corresponding to the given plaintext.
	// Encryption in-place is allowed within original slice's capacity. Encryption parameters are passed in ObjectID.
	Encrypt(plainText []byte, oid ObjectID) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext.
	// Decryption in-place is allowed within original slice's capacity. Encryption parameters are passed in ObjectID.
	Decrypt(cipherText []byte, oid ObjectID) ([]byte, error)
}

// digestFunction computes the digest (hash, optionally HMAC) of a given block of bytes.
type digestFunction func([]byte) []byte

// unencryptedFormat implements non-encrypted format.
type unencryptedFormat struct {
	digestFunc digestFunction
}

func (fi *unencryptedFormat) ComputeObjectID(data []byte) ObjectID {
	h := fi.digestFunc(data)

	return ObjectID{StorageBlock: hex.EncodeToString(h)}
}

func (fi *unencryptedFormat) Encrypt(plainText []byte, oid ObjectID) ([]byte, error) {
	return plainText, nil
}

func (fi *unencryptedFormat) Decrypt(cipherText []byte, oid ObjectID) ([]byte, error) {
	return cipherText, nil
}

// syntheticIVEncryptionFormat implements encrypted format with single master AES key and StorageBlock==IV that's
// derived from HMAC-SHA256(content, secret).
type syntheticIVEncryptionFormat struct {
	digestFunc   digestFunction
	createCipher func(key []byte) (cipher.Block, error)
	aesKey       []byte
}

func (fi *syntheticIVEncryptionFormat) ComputeObjectID(data []byte) ObjectID {
	h := fi.digestFunc(data)
	return ObjectID{StorageBlock: hex.EncodeToString(h)}
}

func (fi *syntheticIVEncryptionFormat) Encrypt(plainText []byte, oid ObjectID) ([]byte, error) {
	iv, err := decodeHexSuffix(oid.StorageBlock, aes.BlockSize*2)
	if err != nil {
		return nil, err
	}

	return symmetricEncrypt(fi.createCipher, fi.aesKey, iv, plainText)
}

func (fi *syntheticIVEncryptionFormat) Decrypt(cipherText []byte, oid ObjectID) ([]byte, error) {
	iv, err := decodeHexSuffix(oid.StorageBlock, aes.BlockSize*2)
	if err != nil {
		return nil, err
	}

	return symmetricEncrypt(fi.createCipher, fi.aesKey, iv, cipherText)
}

func symmetricEncrypt(createCipher func(key []byte) (cipher.Block, error), key []byte, iv []byte, b []byte) ([]byte, error) {
	blockCipher, err := createCipher(key)
	if err != nil {
		return nil, err
	}

	ctr := cipher.NewCTR(blockCipher, iv[0:blockCipher.BlockSize()])
	ctr.XORKeyStream(b, b)
	return b, nil
}

func decodeHexSuffix(s string, length int) ([]byte, error) {
	return hex.DecodeString(s[len(s)-length:])
}

// SupportedFormats is a map with an ObjectFormatter for each supported object format:
//
//   UNENCRYPTED_HMAC_SHA256_128          - unencrypted, block IDs are 128-bit (32 characters long)
//   UNENCRYPTED_HMAC_SHA256              - unencrypted, block IDs are 256-bit (64 characters long)
//   ENCRYPTED_HMAC_SHA256_AES256_SIV     - encrypted with AES-256 (shared key), IV==FOLD(HMAC-SHA256(content), 128)
//
// Additional formats can be supported by adding them to the map.
var SupportedFormats map[string]func(f *Format) (ObjectFormatter, error)

func init() {
	SupportedFormats = map[string]func(f *Format) (ObjectFormatter, error){
		"TESTONLY_MD5": func(f *Format) (ObjectFormatter, error) {
			return &unencryptedFormat{computeHash(md5.New, md5.Size)}, nil
		},
		"UNENCRYPTED_HMAC_SHA256": func(f *Format) (ObjectFormatter, error) {
			return &unencryptedFormat{computeHMAC(sha256.New, f.Secret, sha256.Size)}, nil
		},
		"UNENCRYPTED_HMAC_SHA256_128": func(f *Format) (ObjectFormatter, error) {
			return &unencryptedFormat{computeHMAC(sha256.New, f.Secret, 16)}, nil
		},
		"ENCRYPTED_HMAC_SHA256_AES256_SIV": func(f *Format) (ObjectFormatter, error) {
			if len(f.MasterKey) < 32 {
				return nil, fmt.Errorf("master key is not set")
			}
			return &syntheticIVEncryptionFormat{computeHMAC(sha256.New, f.Secret, aes.BlockSize), aes.NewCipher, f.MasterKey}, nil
		},
	}
}

// DefaultObjectFormat is the format that should be used by default when creating new repositories.
var DefaultObjectFormat = "ENCRYPTED_HMAC_SHA256_AES256_SIV"

// computeHash returns a digestFunction that computes a hash of a given block of bytes and truncates results to the given size.
func computeHash(hf func() hash.Hash, truncate int) digestFunction {
	return func(b []byte) []byte {
		h := hf()
		h.Write(b)
		return h.Sum(nil)[0:truncate]
	}
}

// computeHMAC returns a digestFunction that computes HMAC(hash, secret) of a given block of bytes and truncates results to the given size.
func computeHMAC(hf func() hash.Hash, secret []byte, truncate int) digestFunction {
	return func(b []byte) []byte {
		h := hmac.New(hf, secret)
		h.Write(b)
		return h.Sum(nil)[0:truncate]
	}
}
