// Package secrets keeps user-supplied secrets.
package secrets

// Manage the signing token for all secrets.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/pkg/errors"
	"golang.org/x/crypto/pbkdf2"

	"github.com/fernet/fernet-go"
)

// EncryptedToken holds an encrypted copy of the signing key as well as the corresponding salt.
type EncryptedToken struct { //nolint:musttag
	Token []byte
	Salt  [8]byte
	IsSet bool
}

// String returns a string representation of the encrypted token.
func (t *EncryptedToken) String() string {
	return hex.EncodeToString(t.Token) + "-" + hex.EncodeToString(t.Salt[:])
}

// MarshalJSON will emit an encrypted secret if the original type was Config or Value else "".
func (t EncryptedToken) MarshalJSON() ([]byte, error) {
	//nolint:wrapcheck
	return json.Marshal(t.String())
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

	var d string

	if err := json.Unmarshal(b, &d); err != nil {
		return errors.Wrap(err, "Failed to unmarshal secret")
	}

	dec, err := hex.DecodeString(d[len(d)-16:])
	if err != nil {
		return errors.Wrap(err, "Could not decde salt from hex")
	}

	copy(t.Salt[:], dec)

	dec, err = hex.DecodeString(d[:len(d)-17])
	if err != nil {
		return errors.Wrap(err, "Could not decde token from hex")
	}

	t.Token = make([]byte, len(dec))
	copy(t.Token, dec)

	return nil
}

func (t *EncryptedToken) generateKey(password string) *fernet.Key {
	const rounds = 4096

	const keylen = 32

	key := pbkdf2.Key([]byte(password), t.Salt[:], rounds, keylen, sha256.New)
	fkey := new(fernet.Key)
	copy(fkey[:], key)

	return fkey
}

func (t *EncryptedToken) signingKey(password string) (*fernet.Key, error) {
	// Derive 32-byte key from the password
	key := []*fernet.Key{t.generateKey(password)}

	// Decrypt the encrypted token with the derived-key to generate the signing key
	decryptedToken := fernet.VerifyAndDecrypt(t.Token, 0, key)
	if decryptedToken == nil {
		return nil, errors.New("Failed to decrypt token")
	}

	// Convert signing key into fernet Key
	var tokenAsKey fernet.Key

	copy(tokenAsKey[:], decryptedToken)

	return &tokenAsKey, nil
}

// CreateToken will create a new sigining token and encrypt it with the supplied password.
func CreateToken(password string) (*EncryptedToken, error) {
	encryptedToken := EncryptedToken{}
	token := new(fernet.Key)

	err := token.Generate()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to generate token")
	}

	_, err = rand.Read(encryptedToken.Salt[:])
	if err != nil {
		return nil, errors.Wrap(err, "Failed to genrate salt")
	}

	key := encryptedToken.generateKey(password)

	encryptedToken.Token, err = fernet.EncryptAndSign(token[:], key)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to encrypt token")
	}

	encryptedToken.IsSet = true

	return &encryptedToken, nil
}

// ChangePassword will update the EncryptedToken with a new password.
func (t *EncryptedToken) ChangePassword(oldPassword, newPassword string) error {
	oldKey := []*fernet.Key{t.generateKey(oldPassword)}
	newKey := t.generateKey(newPassword)
	decryptedToken := fernet.VerifyAndDecrypt(t.Token, 0, oldKey)

	tok, err := fernet.EncryptAndSign(decryptedToken, newKey)
	if err != nil {
		return errors.Wrap(err, "Failed to encrypt new password")
	}

	t.Token = tok

	return nil
}
