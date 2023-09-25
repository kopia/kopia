package secrets

import (
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
)

func encrypt(algorithm string, data, key, salt []byte) ([]byte, error) {
	//nolint:wrapcheck
	switch algorithm {
	case "AES256-GCM-HMAC-SHA256":
		return crypto.EncryptAes256Gcm(data, key, salt)
	default:
		return nil, errors.Errorf("Invalid secret algorithm: %v", algorithm)
	}
}

func decrypt(algorithm string, data, key, salt []byte) ([]byte, error) {
	//nolint:wrapcheck
	switch algorithm {
	case "AES256-GCM-HMAC-SHA256":
		return crypto.DecryptAes256Gcm(data, key, salt)
	default:
		return nil, errors.Errorf("Invalid secret algorithm: %v", algorithm)
	}
}
