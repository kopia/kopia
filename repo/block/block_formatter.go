package block

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac" //nolint:gas
	"crypto/sha256"
	"fmt"
	"hash"
	"sort"
	"strings"
)

// Formatter performs data block ID computation and encryption of a block of data when storing object in a repository.
type Formatter interface {
	// ComputeBlockID computes ID of the storage block for the specified block of data and returns it in ObjectID.
	ComputeBlockID(data []byte) []byte

	// Encrypt returns encrypted bytes corresponding to the given plaintext. Must not clobber the input slice.
	Encrypt(plainText []byte, blockID []byte) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext. Must not clobber the input slice.
	Decrypt(cipherText []byte, blockID []byte) ([]byte, error)
}

// digestFunction computes the digest (hash, optionally HMAC) of a given block of bytes.
type digestFunction func([]byte) []byte

// unencryptedFormat implements non-encrypted format.
type unencryptedFormat struct {
	digestFunc digestFunction
}

func (fi *unencryptedFormat) ComputeBlockID(data []byte) []byte {
	return fi.digestFunc(data)
}

func (fi *unencryptedFormat) Encrypt(plainText []byte, blockID []byte) ([]byte, error) {
	return cloneBytes(plainText), nil
}

func (fi *unencryptedFormat) Decrypt(cipherText []byte, blockID []byte) ([]byte, error) {
	return cloneBytes(cipherText), nil
}

// syntheticIVEncryptionFormat implements encrypted format with single master AES key and StorageBlock==IV that's
// derived from HMAC-SHA256(content, secret).
type syntheticIVEncryptionFormat struct {
	digestFunc   digestFunction
	createCipher func(key []byte) (cipher.Block, error)
	aesKey       []byte
}

func (fi *syntheticIVEncryptionFormat) ComputeBlockID(data []byte) []byte {
	return fi.digestFunc(data)
}

func (fi *syntheticIVEncryptionFormat) Encrypt(plainText []byte, blockID []byte) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, fi.aesKey, blockID, plainText)
}

func (fi *syntheticIVEncryptionFormat) Decrypt(cipherText []byte, blockID []byte) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, fi.aesKey, blockID, cipherText)
}

func symmetricEncrypt(createCipher func(key []byte) (cipher.Block, error), key []byte, iv []byte, b []byte) ([]byte, error) {
	blockCipher, err := createCipher(key)
	if err != nil {
		return nil, err
	}

	ctr := cipher.NewCTR(blockCipher, iv[0:blockCipher.BlockSize()])
	result := make([]byte, len(b))
	ctr.XORKeyStream(result, b)
	return result, nil
}

// SupportedFormats is a list of supported object formats including:
//
//   UNENCRYPTED_HMAC_SHA256_128          - unencrypted, block IDs are 128-bit (32 characters long)
//   UNENCRYPTED_HMAC_SHA256              - unencrypted, block IDs are 256-bit (64 characters long)
//   ENCRYPTED_HMAC_SHA256_AES256_SIV     - encrypted with AES-256 (shared key), IV==FOLD(HMAC-SHA256(content), 128)
var SupportedFormats []string

// FormatterFactories maps known block formatters to their factory functions.
var FormatterFactories map[string]func(f FormattingOptions) (Formatter, error)

func init() {
	FormatterFactories = map[string]func(f FormattingOptions) (Formatter, error){
		"UNENCRYPTED_HMAC_SHA256": func(f FormattingOptions) (Formatter, error) {
			return &unencryptedFormat{computeHMAC(sha256.New, f.HMACSecret, sha256.Size)}, nil
		},
		"UNENCRYPTED_HMAC_SHA256_128": func(f FormattingOptions) (Formatter, error) {
			return &unencryptedFormat{computeHMAC(sha256.New, f.HMACSecret, 16)}, nil
		},
		"ENCRYPTED_HMAC_SHA256_AES256_SIV": func(f FormattingOptions) (Formatter, error) {
			if len(f.MasterKey) < 32 {
				return nil, fmt.Errorf("master key is not set")
			}
			return &syntheticIVEncryptionFormat{computeHMAC(sha256.New, f.HMACSecret, aes.BlockSize), aes.NewCipher, f.MasterKey}, nil
		},
	}

	for k := range FormatterFactories {
		if !strings.HasPrefix(k, "TESTONLY_") {
			SupportedFormats = append(SupportedFormats, k)
		}
	}

	sort.Strings(SupportedFormats)
}

// DefaultFormat is the block format that should be used by default when creating new repositories.
const DefaultFormat = "ENCRYPTED_HMAC_SHA256_AES256_SIV"

// computeHMAC returns a digestFunction that computes HMAC(hash, secret) of a given block of bytes and truncates results to the given size.
func computeHMAC(hf func() hash.Hash, secret []byte, truncate int) digestFunction {
	return func(b []byte) []byte {
		h := hmac.New(hf, secret)
		h.Write(b) // nolint:errcheck
		return h.Sum(nil)[0:truncate]
	}
}
