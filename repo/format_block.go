package repo

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/storage"
)

// FormatBlockID is the identifier of a storage block that describes repository format.
const FormatBlockID = "kopia.repository"

var (
	purposeAESKey   = []byte("AES")
	purposeAuthData = []byte("CHECKSUM")
)

type formatBlock struct {
	Tool         string `json:"tool"`
	BuildVersion string `json:"buildVersion"`
	BuildInfo    string `json:"buildInfo"`

	auth.SecurityOptions

	Version              string                         `json:"version"`
	EncryptionAlgorithm  string                         `json:"encryption"`
	EncryptedFormatBytes []byte                         `json:"encryptedBlockFormat,omitempty"`
	UnencryptedFormat    *config.RepositoryObjectFormat `json:"blockFormat,omitempty"`
}

// encryptedRepositoryConfig contains the configuration of repository that's persisted in encrypted format.
type encryptedRepositoryConfig struct {
	Format config.RepositoryObjectFormat `json:"format"`
}

func parseFormatBlock(b []byte) (*formatBlock, error) {
	f := &formatBlock{}

	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("invalid format block: %v", err)
	}

	return f, nil
}

func writeFormatBlock(ctx context.Context, st storage.Storage, f *formatBlock) error {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")
	if err := e.Encode(f); err != nil {
		return fmt.Errorf("unable to marshal format block: %v", err)
	}

	if err := st.PutBlock(ctx, FormatBlockID, buf.Bytes()); err != nil {
		return fmt.Errorf("unable to write format block: %v", err)
	}

	return nil
}

func decryptFormatBytes(f *formatBlock, km *auth.KeyManager) (*config.RepositoryObjectFormat, error) {
	switch f.EncryptionAlgorithm {
	case "NONE": // do nothing
		return f.UnencryptedFormat, nil

	case "AES256_GCM":
		aead, authData, err := initCrypto(km)
		if err != nil {
			return nil, fmt.Errorf("cannot initialize cipher: %v", err)
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
			return nil, fmt.Errorf("invalid repository format: %v", err)
		}

		return &erc.Format, nil

	default:
		return nil, fmt.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

func initCrypto(km *auth.KeyManager) (cipher.AEAD, []byte, error) {
	aesKey := km.DeriveKey(purposeAESKey, 32)
	authData := km.DeriveKey(purposeAuthData, 32)

	blk, err := aes.NewCipher(aesKey)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create cipher: %v", err)
	}
	aead, err := cipher.NewGCM(blk)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create cipher: %v", err)
	}

	return aead, authData, nil
}

func encryptFormatBytes(f *formatBlock, format *config.RepositoryObjectFormat, km *auth.KeyManager) error {
	switch f.EncryptionAlgorithm {
	case "NONE":
		f.UnencryptedFormat = format
		return nil

	case "AES256_GCM":
		content, err := json.Marshal(&encryptedRepositoryConfig{Format: *format})
		if err != nil {
			return fmt.Errorf("can't marshal format to JSON: %v", err)
		}
		aead, authData, err := initCrypto(km)
		if err != nil {
			return fmt.Errorf("unable to initialize crypto: %v", err)
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
