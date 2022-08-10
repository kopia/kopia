package content

import (
	"crypto/aes"
	"encoding/hex"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/hashing"
	"github.com/kopia/kopia/repo/transform"
)

// crypter ecapsulates hashing and encryption.
type crypter interface {
	HashFunc() hashing.HashFunc
	Transformer() transform.Transformer
}

// getIndexBlobIV gets the initialization vector from the provided blob ID by taking
// 32 characters immediately preceding the first dash ('-') and decoding them using base16.
func getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { //nolint:gocritic
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
func EncryptBLOB(c crypter, payload gather.Bytes, prefix blob.ID, sessionID SessionID, output *gather.WriteBuffer) (blob.ID, error) {
	var hashOutput [hashing.MaxHashSize]byte

	hash := c.HashFunc()(hashOutput[:0], payload)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	if sessionID != "" {
		blobID += blob.ID("-" + sessionID)
	}

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return "", err
	}

	output.Reset()

	if err := c.Transformer().ToRepository(payload, iv, output); err != nil {
		return "", errors.Wrapf(err, "error encrypting BLOB %v", blobID)
	}

	return blobID, nil
}

// DecryptBLOB decrypts the provided data using provided blobID to derive initialization vector.
func DecryptBLOB(c crypter, payload gather.Bytes, blobID blob.ID, output *gather.WriteBuffer) error {
	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return errors.Wrap(err, "unable to get index blob IV")
	}

	output.Reset()

	// FromRepository will verify the payload.
	if err := c.Transformer().FromRepository(payload, iv, output); err != nil {
		return errors.Wrapf(err, "error decrypting BLOB %v", blobID)
	}

	return nil
}
