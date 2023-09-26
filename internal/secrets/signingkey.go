// Package secrets keeps user-supplied secrets.
package secrets

// Manage the signing token for all secrets.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
)

// DefaultAlgorithm contains the default encryption algorithm for secrets.
const DefaultAlgorithm = "AES256-GCM-HMAC-SHA256"

// SupportedAlgorithms contains all valid encryption algorithms for secrets.
func SupportedAlgorithms() []string {
	return []string{DefaultAlgorithm}
}

// EncryptedToken holds an encrypted copy of the signing key as well as the corresponding salt.
type EncryptedToken struct { //nolint:musttag
	encryptedKey []byte
	salt         [8]byte
	derivedKey   []byte
	Algorithm    string
	IsSet        bool
}

type keyStruct struct {
	Key       string `json:"key,omitempty"`
	Algorithm string `json:"algorithm,omitempty"`
}

// NewSigningKey generates storage for a new key without generating the key itself.
func NewSigningKey(algorithm string) *EncryptedToken {
	signingKey := EncryptedToken{Algorithm: algorithm}
	return &signingKey
}

// String returns a string representation of the encrypted token.
func (t *EncryptedToken) String() string {
	return hex.EncodeToString(t.encryptedKey) + "-" + hex.EncodeToString(t.salt[:])
}

// MarshalJSON will emit an encrypted secret if the original type was Config or Value else "".
func (t EncryptedToken) MarshalJSON() ([]byte, error) {
	if !t.IsSet {
		//nolint:wrapcheck
		return json.Marshal(nil)
	}

	d := keyStruct{Key: t.String(), Algorithm: t.Algorithm}

	//nolint:wrapcheck
	return json.Marshal(d)
}

// UnmarshalJSON parses octal permissions string from JSON.
func (t *EncryptedToken) UnmarshalJSON(b []byte) error {
	const minLen = 81 // (8+32)*2 + 1

	if b == nil {
		return nil
	}

	if len(b) < minLen {
		return errors.New("Improper data length for token")
	}

	var d keyStruct

	if err := json.Unmarshal(b, &d); err != nil {
		return errors.Wrap(err, "Failed to unmarshal secret")
	}

	if len(d.Key) < minLen {
		return errors.New("Improper data length for token")
	}

	// Extract the salt.
	dec, err := hex.DecodeString(d.Key[len(d.Key)-16:])
	if err != nil {
		return errors.Wrap(err, "Could not decde salt from hex")
	}

	copy(t.salt[:], dec)

	// Extract the encrypted key
	dec, err = hex.DecodeString(d.Key[:len(d.Key)-17])
	if err != nil {
		return errors.Wrap(err, "Could not decde token from hex")
	}

	t.encryptedKey = dec
	t.Algorithm = d.Algorithm
	t.IsSet = true

	return nil
}

func (t *EncryptedToken) deriveKey(password string) error {
	// Derive 32-byte key from the password
	key, err := crypto.DeriveKeyFromPassword(password, t.salt[:], crypto.DefaultKeyDerivationAlgorithm)
	if err != nil {
		return errors.Wrap(err, "Failed to derive key")
	}

	t.derivedKey = key

	return nil
}

func (t *EncryptedToken) getDerivedKey(password string) ([]byte, error) {
	var err error

	if t.derivedKey == nil {
		if !t.IsSet {
			err = t.Create(password)
		} else {
			err = t.deriveKey(password)
		}

		if err != nil {
			return nil, err
		}
	}

	return t.derivedKey, nil
}

// encryptKey will generate an encrypted signing key from a provided key and password.
func (t *EncryptedToken) encryptKey(signingKey []byte, password string) error {
	key, err := t.getDerivedKey(password)
	if err != nil {
		return err
	}

	// Decrypt the encrypted token with the derived-key to generate the signing key
	encryptedKey, err := encrypt(t.Algorithm, signingKey, key, t.salt[:])
	if err != nil {
		return errors.Wrap(err, "Failed to decrypt signing key")
	}

	t.encryptedKey = encryptedKey

	return nil
}

func (t *EncryptedToken) signingKey(password string) ([]byte, error) {
	key, err := t.getDerivedKey(password)
	if err != nil {
		return nil, err
	}

	// Decrypt the encrypted token with the derived-key to generate the signing key
	signingKey, err := decrypt(t.Algorithm, t.encryptedKey, key, t.salt[:])
	if err != nil {
		return nil, errors.Wrap(err, "Failed to decrypt signing key")
	}

	return signingKey, nil
}

// Create will create a new sigining token and encrypt it with the supplied password.
func (t *EncryptedToken) Create(password string) error {
	var signingKey [32]byte

	// Generate a random 32-byte signining key
	if _, err := rand.Read(signingKey[:]); err != nil {
		return errors.Wrap(err, "Failed to genrate signing key")
	}

	if _, err := rand.Read(t.salt[:]); err != nil {
		return errors.Wrap(err, "Failed to genrate salt")
	}

	if err := t.deriveKey(password); err != nil {
		return err
	}

	if err := t.encryptKey(signingKey[:], password); err != nil {
		return err
	}

	t.IsSet = true

	return nil
}

// ChangePassword will update the EncryptedToken with a new password.
func (t *EncryptedToken) ChangePassword(oldPassword, newPassword string) error {
	key, err := t.signingKey(oldPassword)
	if err != nil {
		return err
	}

	t.derivedKey = nil

	err = t.encryptKey(key, newPassword)

	return err
}

// Encrypt will encrypt data with the sigining key.
func (t *EncryptedToken) Encrypt(data []byte, password string) ([]byte, error) {
	key, err := t.signingKey(password)
	if err != nil {
		return nil, err
	}

	encrypted, err := encrypt(t.Algorithm, data, key, t.salt[:])
	if err != nil {
		return nil, errors.Wrap(err, "failed to encrypt secret")
	}

	return encrypted, nil
}

// Decrypt will decrypt data with the sigining key.
func (t *EncryptedToken) Decrypt(data []byte, password string) ([]byte, error) {
	key, err := t.signingKey(password)
	if err != nil {
		return nil, err
	}

	decrypted, err := decrypt(t.Algorithm, data, key, t.salt[:])
	if err != nil {
		return nil, errors.Wrap(err, "failed to decrypt secret")
	}

	return decrypted, nil
}
