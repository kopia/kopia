package block

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5" //nolint:gas
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

	// Encrypt returns encrypted bytes corresponding to the given plaintext. May reuse the input slice.
	Encrypt(plainText []byte, blockID []byte, skip int) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext. May reuse the input slice.
	Decrypt(cipherText []byte, blockID []byte, skip int) ([]byte, error)
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

func (fi *unencryptedFormat) Encrypt(plainText []byte, blockID []byte, skip int) ([]byte, error) {
	return plainText, nil
}

func (fi *unencryptedFormat) Decrypt(cipherText []byte, blockID []byte, skip int) ([]byte, error) {
	return cipherText, nil
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

func (fi *syntheticIVEncryptionFormat) Encrypt(plainText []byte, blockID []byte, skip int) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, fi.aesKey, blockID, plainText, skip)
}

func (fi *syntheticIVEncryptionFormat) Decrypt(cipherText []byte, blockID []byte, skip int) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, fi.aesKey, blockID, cipherText, skip)
}

func symmetricEncrypt(createCipher func(key []byte) (cipher.Block, error), key []byte, iv []byte, b []byte, skip int) ([]byte, error) {
	blockCipher, err := createCipher(key)
	if err != nil {
		return nil, err
	}

	ctr := cipher.NewCTR(blockCipher, iv[0:blockCipher.BlockSize()])
	if skip > 0 {
		var skipBuf [32]byte
		skipBufSlice := skipBuf[:]
		for skip >= len(skipBuf) {
			ctr.XORKeyStream(skipBufSlice, skipBufSlice)
			skip -= len(skipBufSlice)
		}
		if skip > 0 {
			ctr.XORKeyStream(skipBufSlice[0:skip], skipBufSlice[0:skip])
		}
	}

	ctr.XORKeyStream(b, b)
	return b, nil
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
		"TESTONLY_MD5": func(f FormattingOptions) (Formatter, error) {
			return &unencryptedFormat{computeHash(md5.New, md5.Size)}, nil
		},
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

// computeHash returns a digestFunction that computes a hash of a given block of bytes and truncates results to the given size.
func computeHash(hf func() hash.Hash, truncate int) digestFunction {
	return func(b []byte) []byte {
		h := hf()
		h.Write(b) // nolint:errcheck
		return h.Sum(nil)[0:truncate]
	}
}

// computeHMAC returns a digestFunction that computes HMAC(hash, secret) of a given block of bytes and truncates results to the given size.
func computeHMAC(hf func() hash.Hash, secret []byte, truncate int) digestFunction {
	return func(b []byte) []byte {
		h := hmac.New(hf, secret)
		h.Write(b) // nolint:errcheck
		return h.Sum(nil)[0:truncate]
	}
}
