package content

import (
	"bytes"
	"crypto/aes"
	"encoding/hex"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

func getIndexBlobIV(s blob.ID) ([]byte, error) {
	if p := strings.Index(string(s), "-"); p >= 0 { // nolint:gocritic
		s = s[0:p]
	}

	if len(s) < 2*aes.BlockSize {
		return nil, errors.Errorf("blob id too short: %v", s)
	}

	return hex.DecodeString(string(s[len(s)-(aes.BlockSize*2):])) //nolint:gomnd
}

func encryptFullBlob(h hashing.HashFunc, enc encryption.Encryptor, data []byte, prefix blob.ID, sessionID SessionID) (blob.ID, []byte, error) {
	var hashOutput [maxHashSize]byte

	hash := h(hashOutput[:0], data)
	blobID := prefix + blob.ID(hex.EncodeToString(hash))

	if sessionID != "" {
		blobID += blob.ID("-" + sessionID)
	}

	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return "", nil, err
	}

	data2, err := enc.Encrypt(nil, data, iv)
	if err != nil {
		return "", nil, errors.Wrapf(err, "error encrypting blob %v", blobID)
	}

	return blobID, data2, nil
}

func decryptFullBlob(h hashing.HashFunc, enc encryption.Encryptor, payload []byte, blobID blob.ID) ([]byte, error) {
	iv, err := getIndexBlobIV(blobID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get index blob IV")
	}

	payload, err = enc.Decrypt(nil, payload, iv)
	if err != nil {
		return nil, errors.Wrap(err, "decrypt error")
	}

	// Since the encryption key is a function of data, we must be able to generate exactly the same key
	// after decrypting the content. This serves as a checksum.
	if err := verifyChecksum(h, payload, iv); err != nil {
		return nil, err
	}

	return payload, nil
}

func verifyChecksum(h hashing.HashFunc, data, iv []byte) error {
	var hashOutput [maxHashSize]byte

	expected := h(hashOutput[:0], data)
	expected = expected[len(expected)-aes.BlockSize:]

	if !bytes.HasSuffix(iv, expected) {
		return errors.Errorf("invalid checksum for blob %x, expected %x", iv, expected)
	}

	return nil
}
