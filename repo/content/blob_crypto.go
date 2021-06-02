package content

import (
	"crypto/aes"
	"encoding/hex"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// Crypter ecapsulates hashing and encryption and provides utilities for whole-BLOB encryption.
// Whole-BLOB encryption relies on BLOB identifiers formatted as:
//
// <prefix><hash>[-optionalSuffix]
//
// Where:
//   'prefix' is arbitrary string without dashes
//   'hash' is base16-encoded 128-bit hash of contents, used as initialization vector (IV)
//          for the encryption. In case of longer hash functions, we use last 16 bytes of
//          their outputs.
//   'optionalSuffix' can be any string
type Crypter struct {
	HashFunction hashing.HashFunc
	Encryptor    encryption.Encryptor
}

// getIndexBlobIV gets the initialization vector from the provided blob ID by taking
// 32 characters immediately preceding the first dash ('-') and decoding them using base16.
func (c *Crypter) getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { // nolint:gocritic
		s = s[0:p]
	}

	if len(s) < 2*aes.BlockSize {
		return nil, errors.Errorf("blob id too short: %v", s)
	}

	v, err := hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):])) //nolint:gomnd
	if err != nil {
		return nil, errors.Errorf("invalid blob ID: %v", s)
	}

	return v, nil
}

// EncryptBLOB encrypts the given data using crypter-defined key and returns a name that should
// be used to save the blob in thre repository.
func (c *Crypter) EncryptBLOB(data []byte, prefix blob.ID, sessionID SessionID) (blob.ID, []byte, error) {
	var hashOutput [maxHashSize]byte

	hash := c.HashFunction(hashOutput[:0], data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	if sessionID != "" {
		blobID += blob.ID("-" + sessionID)
	}

	iv, err := c.getIndexBlobIV(blobID)
	if err != nil {
		return "", nil, err
	}

	data2, err := c.Encryptor.Encrypt(nil, data, iv)
	if err != nil {
		return "", nil, errors.Wrapf(err, "error encrypting BLOB %v", blobID)
	}

	return blobID, data2, nil
}

// DecryptBLOB decrypts the provided data using provided blobID to derive initialization vector.
func (c *Crypter) DecryptBLOB(payload []byte, blobID blob.ID) ([]byte, error) {
	iv, err := c.getIndexBlobIV(blobID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get index blob IV")
	}

	// Decrypt will verify the payload.
	payload, err = c.Encryptor.Decrypt(nil, payload, iv)
	if err != nil {
		return nil, errors.Wrapf(err, "error decrypting BLOB %v", blobID)
	}

	return payload, nil
}
