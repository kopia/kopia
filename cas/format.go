package cas

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"io"
)

// Format describes the format of object data.
type Format struct {
	Version           string `json:"version"`
	ObjectFormat      string `json:"objectFormat"`
	Secret            []byte `json:"secret,omitempty"`
	MaxInlineBlobSize int    `json:"maxInlineBlobSize"`
	MaxBlobSize       int    `json:"maxBlobSize"`
}

func NewFormat() (*Format, error) {
	f := &Format{
		Version:      "1",
		Secret:       make([]byte, 32),
		ObjectFormat: "hmac-sha256",
	}

	_, err := io.ReadFull(rand.Reader, f.Secret)
	if err != nil {
		return f, err
	}
	return f, nil
}

// ObjectIDFormat describes single format ObjectID
type ObjectIDFormat struct {
	Name string

	hashFuncMaker func(secret []byte) func() hash.Hash
	createCipher  func([]byte) (cipher.Block, error)
	keygen        keygenFunc
}

// IsEncrypted determines whether the ObjectIDFormat is encrypted.
func (oif *ObjectIDFormat) IsEncrypted() bool {
	return oif.createCipher != nil
}

// HashSizeBits returns the number of bits returned by hash function.
func (oif *ObjectIDFormat) HashSizeBits() int {
	hf := oif.hashFuncMaker(nil)
	return hf().Size() * 8
}

// BlockIDLength returns the number of characters in a stored block ID.
func (oif *ObjectIDFormat) BlockIDLength() int {
	hf := oif.hashFuncMaker(nil)
	if oif.keygen == nil {
		return hf().Size() * 2
	}

	h := hf().Sum(nil)
	blockID, _ := oif.keygen(h)
	return len(blockID) * 2
}

// EncryptionKeySizeBits returns the size of encryption key in bits,
// or zero if no encryption is used in this format.
func (oif *ObjectIDFormat) EncryptionKeySizeBits() int {
	if oif.keygen == nil {
		return 0
	}
	hf := oif.hashFuncMaker(nil)
	h := hf().Sum(nil)
	_, key := oif.keygen(h)
	return len(key) * 8
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

// SupportedFormats contains supported repository formats.
var SupportedFormats = ObjectIDFormats{
	// Non-encrypted formats.
	&ObjectIDFormat{"md5", nonHMAC(md5.New), nil, nil},
	&ObjectIDFormat{"hmac-md5", withHMAC(md5.New), nil, nil},
	&ObjectIDFormat{"sha1", nonHMAC(sha1.New), nil, nil},
	&ObjectIDFormat{"hmac-sha1", withHMAC(sha1.New), nil, nil},
	&ObjectIDFormat{"sha256", nonHMAC(sha256.New), nil, nil},
	&ObjectIDFormat{"hmac-sha256", withHMAC(sha256.New), nil, nil},
	&ObjectIDFormat{"sha512-256", nonHMAC(sha512.New512_256), nil, nil},
	&ObjectIDFormat{"hmac-sha512-256", withHMAC(sha512.New512_256), nil, nil},
	&ObjectIDFormat{"sha512", nonHMAC(sha512.New), nil, nil},
	&ObjectIDFormat{"hmac-sha512", withHMAC(sha512.New), nil, nil},
	&ObjectIDFormat{"sha256-trunc128", nonHMAC(sha256.New), nil, splitKeyGenerator(16, 0)},
	&ObjectIDFormat{"hmac-sha256-trunc128", withHMAC(sha256.New), nil, splitKeyGenerator(16, 0)},
	&ObjectIDFormat{"sha512-trunc128", nonHMAC(sha512.New), nil, splitKeyGenerator(16, 0)},
	&ObjectIDFormat{"hmac-sha512-trunc128", withHMAC(sha512.New), nil, splitKeyGenerator(16, 0)},

	// Encrypted formats
	&ObjectIDFormat{"hmac-sha512-aes256", withHMAC(sha512.New), aes.NewCipher, splitKeyGenerator(32, 32)},
	&ObjectIDFormat{"hmac-sha384-aes256", withHMAC(sha512.New384), aes.NewCipher, splitKeyGenerator(16, 32)},
	&ObjectIDFormat{"hmac-sha256-aes128", withHMAC(sha256.New), aes.NewCipher, splitKeyGenerator(16, 16)},
	&ObjectIDFormat{"hmac-sha512-256-aes128", withHMAC(sha512.New512_256), aes.NewCipher, splitKeyGenerator(16, 16)},
}
