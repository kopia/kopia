package repo

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
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
	MaxBlockSize           int32  `json:"maxBlockSize,omitempty"`           // maximum size of storage block
	MasterKey              []byte `json:"masterKey,omitempty"`              // master encryption key (SIV-mode encryption only)
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

// unencryptedFormat implements non-encrypted format.
type unencryptedFormat struct {
	hashCtor func() hash.Hash
	fold     int
	secret   []byte
}

func (fi *unencryptedFormat) ComputeObjectID(data []byte) ObjectID {
	h := hashContent(fi.hashCtor, data, fi.secret)
	if fi.fold > 0 {
		h = fold(h, fi.fold)
	}

	return ObjectID{StorageBlock: hex.EncodeToString(h)}
}

func (fi *unencryptedFormat) Encrypt(plainText []byte, oid ObjectID) ([]byte, error) {
	return plainText, nil
}

func (fi *unencryptedFormat) Decrypt(cipherText []byte, oid ObjectID) ([]byte, error) {
	return cipherText, nil
}

// Since we never share keys, using constant IV is fine.
// Instead of using all-zero, we use this one.
var constantIV = []byte("kopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopiakopia")

// convergentEncryptionFormat implements encrypted format where encryption key is derived from the data and HMAC secret and IV is constant.
// By sharing HMAC secret alone, this allows multiple parties in posession of the same file to generate identical block IDs and encryption keys.
type convergentEncryptionFormat struct {
	hashCtor     func() hash.Hash
	createCipher func(key []byte) (cipher.Block, error)
	keyBytes     int
	secret       []byte
}

func (fi *convergentEncryptionFormat) ComputeObjectID(data []byte) ObjectID {
	h := hashContent(fi.hashCtor, data, fi.secret)
	p := len(h) - fi.keyBytes

	return ObjectID{StorageBlock: hex.EncodeToString(h[0:p]), EncryptionKey: h[p:]}
}

func (fi *convergentEncryptionFormat) Encrypt(plainText []byte, oid ObjectID) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, oid.EncryptionKey, constantIV, plainText)
}

func (fi *convergentEncryptionFormat) Decrypt(cipherText []byte, oid ObjectID) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, oid.EncryptionKey, constantIV, cipherText)
}

// syntheticIVEncryptionFormat implements encrypted format with single master AES key and StorageBlock==IV that's
// derived from HMAC-SHA256(content, secret).
type syntheticIVEncryptionFormat struct {
	createCipher func(key []byte) (cipher.Block, error)
	aesKey       []byte
	hmacSecret   []byte
}

func (fi *syntheticIVEncryptionFormat) ComputeObjectID(data []byte) ObjectID {
	h := hashContent(sha256.New, data, fi.hmacSecret)

	// Fold the bits into 16 bytes required for the IV.
	for i := aes.BlockSize; i < len(h); i++ {
		h[i%aes.BlockSize] ^= h[i]
	}

	h = h[0:aes.BlockSize]
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
//   ENCRYPTED_HMAC_SHA512_384_AES256     - encrypted with AES-256, block IDs are 128-bit (32 characters long)
//   ENCRYPTED_HMAC_SHA512_AES256         - encrypted with AES-256, block IDs are 256-bit (64 characters long)
//   ENCRYPTED_HMAC_SHA256_AES256_SIV     - encrypted with AES-256 (shared key), IV==FOLD(HMAC-SHA256(content), 128)
//
// Additional formats can be supported by adding them to the map.
var SupportedFormats map[string]func(f *Format) (ObjectFormatter, error)

func init() {
	SupportedFormats = map[string]func(f *Format) (ObjectFormatter, error){
		"TESTONLY_MD5": func(f *Format) (ObjectFormatter, error) {
			return &unencryptedFormat{md5.New, 0, f.Secret}, nil
		},
		"UNENCRYPTED_HMAC_SHA256": func(f *Format) (ObjectFormatter, error) {
			return &unencryptedFormat{sha256.New, 0, f.Secret}, nil
		},
		"UNENCRYPTED_HMAC_SHA256_128": func(f *Format) (ObjectFormatter, error) {
			return &unencryptedFormat{sha256.New, 16, f.Secret}, nil
		},
		"ENCRYPTED_HMAC_SHA512_384_AES256": func(f *Format) (ObjectFormatter, error) {
			return &convergentEncryptionFormat{sha512.New384, aes.NewCipher, 32, f.Secret}, nil
		},
		"ENCRYPTED_HMAC_SHA512_AES256": func(f *Format) (ObjectFormatter, error) {
			return &convergentEncryptionFormat{sha512.New, aes.NewCipher, 32, f.Secret}, nil
		},
		"ENCRYPTED_HMAC_SHA256_AES256_SIV": func(f *Format) (ObjectFormatter, error) {
			if len(f.MasterKey) < 32 {
				return nil, fmt.Errorf("master key is not set")
			}
			return &syntheticIVEncryptionFormat{aes.NewCipher, f.MasterKey, f.Secret}, nil
		},
	}
}

// DefaultObjectFormat is the format that should be used by default when creating new repositories.
var DefaultObjectFormat = "ENCRYPTED_HMAC_SHA256_AES256_SIV"

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

	if len(secret) > 0 {
		h = hmac.New(hf, secret)
	} else {
		h = hf()
	}
	h.Write(data)
	return h.Sum(nil)
}
