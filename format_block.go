package repo

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"

	"github.com/kopia/repo/storage"
	"github.com/pkg/errors"
)

const defaultFormatEncryption = "AES256_GCM"

const (
	maxChecksummedFormatBytesLength = 65000
	formatBlockChecksumSize         = sha256.Size
)

// formatBlockChecksumSecret is a HMAC secret used for checksumming the format block.
// It's not really a secret, but will provide positive identification of blocks that
// are repository format blocks.
var formatBlockChecksumSecret = []byte("kopia-repository")

// FormatBlockID is the identifier of a storage block that describes repository format.
const FormatBlockID = "kopia.repository"

var (
	purposeAESKey   = []byte("AES")
	purposeAuthData = []byte("CHECKSUM")

	errFormatBlockNotFound = errors.New("format block not found")
)

type formatBlock struct {
	Tool         string `json:"tool"`
	BuildVersion string `json:"buildVersion"`
	BuildInfo    string `json:"buildInfo"`

	UniqueID               []byte `json:"uniqueID"`
	KeyDerivationAlgorithm string `json:"keyAlgo"`

	Version              string                  `json:"version"`
	EncryptionAlgorithm  string                  `json:"encryption"`
	EncryptedFormatBytes []byte                  `json:"encryptedBlockFormat,omitempty"`
	UnencryptedFormat    *repositoryObjectFormat `json:"blockFormat,omitempty"`
}

// encryptedRepositoryConfig contains the configuration of repository that's persisted in encrypted format.
type encryptedRepositoryConfig struct {
	Format repositoryObjectFormat `json:"format"`
}

func parseFormatBlock(b []byte) (*formatBlock, error) {
	f := &formatBlock{}

	if err := json.Unmarshal(b, &f); err != nil {
		return nil, errors.Wrap(err, "invalid format block")
	}

	return f, nil
}

// RecoverFormatBlock attempts to recover format block replica from the specified file.
// The format block can be either the prefix or a suffix of the given file.
// optionally the length can be provided (if known) to speed up recovery.
func RecoverFormatBlock(ctx context.Context, st storage.Storage, filename string, optionalLength int64) ([]byte, error) {
	if optionalLength > 0 {
		return recoverFormatBlockWithLength(ctx, st, filename, optionalLength)
	}

	var foundMetadata storage.BlockMetadata

	if err := st.ListBlocks(ctx, filename, func(bm storage.BlockMetadata) error {
		if foundMetadata.BlockID != "" {
			return fmt.Errorf("found multiple blocks with a given prefix: %v", filename)
		}
		foundMetadata = bm
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error")
	}

	if foundMetadata.BlockID == "" {
		return nil, storage.ErrBlockNotFound
	}

	return recoverFormatBlockWithLength(ctx, st, foundMetadata.BlockID, foundMetadata.Length)
}

func recoverFormatBlockWithLength(ctx context.Context, st storage.Storage, filename string, length int64) ([]byte, error) {
	chunkLength := int64(65536)
	if chunkLength > length {
		chunkLength = length
	}

	if chunkLength > 4 {

		// try prefix
		prefixChunk, err := st.GetBlock(ctx, filename, 0, chunkLength)
		if err != nil {
			return nil, err
		}
		if l := int(prefixChunk[0]) + int(prefixChunk[1])<<8; l <= maxChecksummedFormatBytesLength && l+2 < len(prefixChunk) {
			if b, ok := verifyFormatBlockChecksum(prefixChunk[2 : 2+l]); ok {
				return b, nil
			}
		}

		// try the suffix
		suffixChunk, err := st.GetBlock(ctx, filename, length-chunkLength, chunkLength)
		if err != nil {
			return nil, err
		}
		if l := int(suffixChunk[len(suffixChunk)-2]) + int(suffixChunk[len(suffixChunk)-1])<<8; l <= maxChecksummedFormatBytesLength && l+2 < len(suffixChunk) {
			if b, ok := verifyFormatBlockChecksum(suffixChunk[len(suffixChunk)-2-l : len(suffixChunk)-2]); ok {
				return b, nil
			}
		}
	}

	return nil, errFormatBlockNotFound
}

func verifyFormatBlockChecksum(b []byte) ([]byte, bool) {
	if len(b) < formatBlockChecksumSize {
		return nil, false
	}

	data, checksum := b[0:len(b)-formatBlockChecksumSize], b[len(b)-formatBlockChecksumSize:]
	h := hmac.New(sha256.New, formatBlockChecksumSecret)
	h.Write(data) //nolint:errcheck
	actualChecksum := h.Sum(nil)
	if !hmac.Equal(actualChecksum, checksum) {
		return nil, false
	}

	return data, true
}

func writeFormatBlock(ctx context.Context, st storage.Storage, f *formatBlock) error {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")
	if err := e.Encode(f); err != nil {
		return errors.Wrap(err, "unable to marshal format block")
	}

	if err := st.PutBlock(ctx, FormatBlockID, buf.Bytes()); err != nil {
		return errors.Wrap(err, "unable to write format block")
	}

	return nil
}

func (f *formatBlock) decryptFormatBytes(masterKey []byte) (*repositoryObjectFormat, error) {
	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		return f.UnencryptedFormat, nil

	case "AES256_GCM":
		aead, authData, err := initCrypto(masterKey, f.UniqueID)
		if err != nil {
			return nil, errors.Wrap(err, "cannot initialize cipher")
		}

		content := append([]byte(nil), f.EncryptedFormatBytes...)
		if len(content) < aead.NonceSize() {
			return nil, fmt.Errorf("invalid encrypted payload, too short")
		}
		nonce := content[0:aead.NonceSize()]
		payload := content[aead.NonceSize():]

		plainText, err := aead.Open(payload[:0], nonce, payload, authData)
		if err != nil {
			return nil, fmt.Errorf("unable to decrypt repository format, invalid credentials?")
		}

		var erc encryptedRepositoryConfig
		if err := json.Unmarshal(plainText, &erc); err != nil {
			return nil, errors.Wrap(err, "invalid repository format")
		}

		return &erc.Format, nil

	default:
		return nil, fmt.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func initCrypto(masterKey, repositoryID []byte) (cipher.AEAD, []byte, error) {
	aesKey := deriveKeyFromMasterKey(masterKey, repositoryID, purposeAESKey, 32)
	authData := deriveKeyFromMasterKey(masterKey, repositoryID, purposeAuthData, 32)

	blk, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create cipher")
	}
	aead, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create cipher")
	}

	return aead, authData, nil
}

func encryptFormatBytes(f *formatBlock, format *repositoryObjectFormat, masterKey, repositoryID []byte) error {
	switch f.EncryptionAlgorithm {
	case "NONE":
		f.UnencryptedFormat = format
		return nil

	case "AES256_GCM":
		content, err := json.Marshal(&encryptedRepositoryConfig{Format: *format})
		if err != nil {
			return errors.Wrap(err, "can't marshal format to JSON")
		}
		aead, authData, err := initCrypto(masterKey, repositoryID)
		if err != nil {
			return errors.Wrap(err, "unable to initialize crypto")
		}
		nonceLength := aead.NonceSize()
		noncePlusContentLength := nonceLength + len(content)
		cipherText := make([]byte, noncePlusContentLength+aead.Overhead())

		// Store nonce at the beginning of ciphertext.
		nonce := cipherText[0:nonceLength]
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}

		b := aead.Seal(cipherText[nonceLength:nonceLength], nonce, content, authData)
		content = nonce[0 : nonceLength+len(b)]
		f.EncryptedFormatBytes = content
		return nil

	default:
		return fmt.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func addFormatBlockChecksumAndLength(fb []byte) ([]byte, error) {
	h := hmac.New(sha256.New, formatBlockChecksumSecret)
	h.Write(fb) //nolint:errcheck
	checksummedFormatBytes := h.Sum(fb)

	l := len(checksummedFormatBytes)
	if l > maxChecksummedFormatBytesLength {
		return nil, fmt.Errorf("format block too big: %v", l)
	}

	// return <length><checksummed-bytes><length>
	result := append([]byte(nil), byte(l), byte(l>>8))
	result = append(result, checksummedFormatBytes...)
	result = append(result, byte(l), byte(l>>8))
	return result, nil
}
