package block

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac" //nolint:gas
	"crypto/sha256"
	"fmt"
	"hash"
	"sort"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/blake2s"
	"golang.org/x/crypto/salsa20"
	"golang.org/x/crypto/sha3"
)

// HashFunc computes hash of block of data using a cryptographic hash function, possibly with HMAC and/or truncation.
type HashFunc func(data []byte) []byte

// HashFuncFactory returns a hash function for given formatting options.
type HashFuncFactory func(o FormattingOptions) (HashFunc, error)

// Encryptor performs encryption and decryption of blocks of data.
type Encryptor interface {
	// Encrypt returns encrypted bytes corresponding to the given plaintext. Must not clobber the input slice.
	Encrypt(plainText []byte, blockID []byte) ([]byte, error)

	// Decrypt returns unencrypted bytes corresponding to the given ciphertext. Must not clobber the input slice.
	Decrypt(cipherText []byte, blockID []byte) ([]byte, error)
}

// EncryptorFactory creates new Encryptor for given FormattingOptions
type EncryptorFactory func(o FormattingOptions) (Encryptor, error)

var hashFunctions = map[string]HashFuncFactory{}
var encryptors = map[string]EncryptorFactory{}

// nullEncryptor implements non-encrypted format.
type nullEncryptor struct {
}

func (fi nullEncryptor) Encrypt(plainText []byte, blockID []byte) ([]byte, error) {
	return cloneBytes(plainText), nil
}

func (fi nullEncryptor) Decrypt(cipherText []byte, blockID []byte) ([]byte, error) {
	return cloneBytes(cipherText), nil
}

// ctrEncryptor implements encrypted format which uses CTR mode of a block cipher with nonce==IV.
type ctrEncryptor struct {
	createCipher func() (cipher.Block, error)
}

func (fi ctrEncryptor) Encrypt(plainText []byte, blockID []byte) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, blockID, plainText)
}

func (fi ctrEncryptor) Decrypt(cipherText []byte, blockID []byte) ([]byte, error) {
	return symmetricEncrypt(fi.createCipher, blockID, cipherText)
}

func symmetricEncrypt(createCipher func() (cipher.Block, error), iv []byte, b []byte) ([]byte, error) {
	blockCipher, err := createCipher()
	if err != nil {
		return nil, err
	}

	ctr := cipher.NewCTR(blockCipher, iv[0:blockCipher.BlockSize()])
	result := make([]byte, len(b))
	ctr.XORKeyStream(result, b)
	return result, nil
}

type salsaEncryptor struct {
	nonceSize int
	key       *[32]byte
}

func (s salsaEncryptor) Decrypt(input []byte, blockID []byte) ([]byte, error) {
	return s.encryptDecrypt(input, blockID)
}

func (s salsaEncryptor) Encrypt(input []byte, blockID []byte) ([]byte, error) {
	return s.encryptDecrypt(input, blockID)
}

func (s salsaEncryptor) encryptDecrypt(input []byte, blockID []byte) ([]byte, error) {
	if len(blockID) < s.nonceSize {
		return nil, fmt.Errorf("hash too short, expected >=%v bytes, got %v", s.nonceSize, len(blockID))
	}
	result := make([]byte, len(input))
	nonce := blockID[0:s.nonceSize]
	salsa20.XORKeyStream(result, input, nonce, s.key)
	return result, nil
}

// truncatedHMACHashFuncFactory returns a HashFuncFactory that computes HMAC(hash, secret) of a given block of bytes
// and truncates results to the given size.
func truncatedHMACHashFuncFactory(hf func() hash.Hash, truncate int) HashFuncFactory {
	return func(o FormattingOptions) (HashFunc, error) {
		return func(b []byte) []byte {
			h := hmac.New(hf, o.HMACSecret)
			h.Write(b) // nolint:errcheck
			return h.Sum(nil)[0:truncate]
		}, nil
	}
}

// truncatedKeyedHashFuncFactory returns a HashFuncFactory that computes keyed hash of a given block of bytes
// and truncates results to the given size.
func truncatedKeyedHashFuncFactory(hf func(key []byte) (hash.Hash, error), truncate int) HashFuncFactory {
	return func(o FormattingOptions) (HashFunc, error) {
		if _, err := hf(o.HMACSecret); err != nil {
			return nil, err
		}

		return func(b []byte) []byte {
			h, _ := hf(o.HMACSecret)
			h.Write(b) // nolint:errcheck
			return h.Sum(nil)[0:truncate]
		}, nil
	}
}

// newCTREncryptorFactory returns new EncryptorFactory that uses CTR with symmetric encryption (such as AES) and a given key size.
func newCTREncryptorFactory(keySize int, createCipherWithKey func(key []byte) (cipher.Block, error)) EncryptorFactory {
	return func(o FormattingOptions) (Encryptor, error) {
		key, err := adjustKey(o.MasterKey, keySize)
		if err != nil {
			return nil, fmt.Errorf("unable to get encryption key: %v", err)
		}

		return ctrEncryptor{
			createCipher: func() (cipher.Block, error) {
				return createCipherWithKey(key)
			},
		}, nil
	}
}

// RegisterHash registers a hash function with a given name.
func RegisterHash(name string, newHashFunc HashFuncFactory) {
	hashFunctions[name] = newHashFunc
}

func SupportedHashAlgorithms() []string {
	var result []string
	for k := range hashFunctions {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

func SupportedEncryptionAlgorithms() []string {
	var result []string
	for k := range encryptors {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

// RegisterEncryption registers new encryption algorithm.
func RegisterEncryption(name string, newEncryptor EncryptorFactory) {
	encryptors[name] = newEncryptor
}

// DefaultHash is the name of the default hash algorithm.
const DefaultHash = "BLAKE2B-256-128"

// DefaultEncryption is the name of the default encryption algorithm.
const DefaultEncryption = "SALSA20"

func init() {
	RegisterHash("HMAC-SHA256", truncatedHMACHashFuncFactory(sha256.New, 32))
	RegisterHash("HMAC-SHA256-128", truncatedHMACHashFuncFactory(sha256.New, 16))
	RegisterHash("HMAC-SHA224", truncatedHMACHashFuncFactory(sha256.New224, 28))
	RegisterHash("HMAC-SHA3-224", truncatedHMACHashFuncFactory(sha3.New224, 28))
	RegisterHash("HMAC-SHA3-256", truncatedHMACHashFuncFactory(sha3.New256, 32))

	RegisterHash("BLAKE2S-128", truncatedKeyedHashFuncFactory(blake2s.New128, 16))
	RegisterHash("BLAKE2S-256", truncatedKeyedHashFuncFactory(blake2s.New256, 32))
	RegisterHash("BLAKE2B-256-128", truncatedKeyedHashFuncFactory(blake2b.New256, 16))
	RegisterHash("BLAKE2B-256", truncatedKeyedHashFuncFactory(blake2b.New256, 32))

	RegisterEncryption("NONE", func(f FormattingOptions) (Encryptor, error) {
		return nullEncryptor{}, nil
	})
	RegisterEncryption("AES-128-CTR", newCTREncryptorFactory(16, aes.NewCipher))
	RegisterEncryption("AES-192-CTR", newCTREncryptorFactory(24, aes.NewCipher))
	RegisterEncryption("AES-256-CTR", newCTREncryptorFactory(32, aes.NewCipher))
	RegisterEncryption("SALSA20", func(f FormattingOptions) (Encryptor, error) {
		var k [32]byte
		copy(k[:], f.MasterKey[0:32])
		return salsaEncryptor{8, &k}, nil
	})
	RegisterEncryption("XSALSA20", func(f FormattingOptions) (Encryptor, error) {
		var k [32]byte
		copy(k[:], f.MasterKey[0:32])
		return salsaEncryptor{24, &k}, nil
	})
}

func adjustKey(masterKey []byte, desiredKeySize int) ([]byte, error) {
	if len(masterKey) == desiredKeySize {
		return masterKey, nil
	}

	if desiredKeySize < len(masterKey) {
		return masterKey[0:desiredKeySize], nil
	}

	return nil, fmt.Errorf("required key too long %v, but only have %v", desiredKeySize, len(masterKey))
}
