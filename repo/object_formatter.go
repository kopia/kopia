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

	"strings"

	"sort"

	"github.com/kopia/kopia/internal/config"
)

// validateFormat checks the validity of RepositoryObjectFormat and returns an error if invalid.
func validateFormat(f *config.RepositoryObjectFormat) error {
	if f.Version != 1 {
		return fmt.Errorf("unsupported version: %v", f.Version)
	}

	if f.MaxBlockSize < 100 {
		return fmt.Errorf("MaxBlockSize is not set")
	}

	if sf := objectFormatterFactories[f.ObjectFormat]; sf == nil {
		return fmt.Errorf("unknown object format: %v", f.ObjectFormat)
	}

	return nil
}

// ObjectFormatter performs data block ID computation and encryption of a block of data when storing object in a repository.
type objectFormatter interface {
	// ComputeObjectID computes ID of the storage block for the specified block of data and returns it in ObjectID.
	ComputeObjectID(data []byte) ObjectID

	// Encrypt returns encrypted bytes corresponding to the given plaintext. May reuse the input slice.
	Encrypt(plainText []byte, oid ObjectID) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext. May reuse the input slice.
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

// SupportedObjectFormats is a list of supported object formats including:
//
//   UNENCRYPTED_HMAC_SHA256_128          - unencrypted, block IDs are 128-bit (32 characters long)
//   UNENCRYPTED_HMAC_SHA256              - unencrypted, block IDs are 256-bit (64 characters long)
//   ENCRYPTED_HMAC_SHA256_AES256_SIV     - encrypted with AES-256 (shared key), IV==FOLD(HMAC-SHA256(content), 128)
var SupportedObjectFormats []string

var objectFormatterFactories = map[string]func(f *config.RepositoryObjectFormat) (objectFormatter, error){
	"TESTONLY_MD5": func(f *config.RepositoryObjectFormat) (objectFormatter, error) {
		return &unencryptedFormat{computeHash(md5.New, md5.Size)}, nil
	},
	"UNENCRYPTED_HMAC_SHA256": func(f *config.RepositoryObjectFormat) (objectFormatter, error) {
		return &unencryptedFormat{computeHMAC(sha256.New, f.HMACSecret, sha256.Size)}, nil
	},
	"UNENCRYPTED_HMAC_SHA256_128": func(f *config.RepositoryObjectFormat) (objectFormatter, error) {
		return &unencryptedFormat{computeHMAC(sha256.New, f.HMACSecret, 16)}, nil
	},
	"ENCRYPTED_HMAC_SHA256_AES256_SIV": func(f *config.RepositoryObjectFormat) (objectFormatter, error) {
		if len(f.MasterKey) < 32 {
			return nil, fmt.Errorf("master key is not set")
		}
		return &syntheticIVEncryptionFormat{computeHMAC(sha256.New, f.HMACSecret, aes.BlockSize), aes.NewCipher, f.MasterKey}, nil
	},
}

func init() {
	for k := range objectFormatterFactories {
		if !strings.HasPrefix(k, "TESTONLY_") {
			SupportedObjectFormats = append(SupportedObjectFormats, k)
		}
	}
	sort.Strings(SupportedObjectFormats)
}

// DefaultObjectFormat is the format that should be used by default when creating new repositories.
const DefaultObjectFormat = "ENCRYPTED_HMAC_SHA256_AES256_SIV"

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
