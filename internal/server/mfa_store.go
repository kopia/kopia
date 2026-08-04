package server

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/natefinch/atomic"
	"github.com/pkg/errors"
	"golang.org/x/crypto/hkdf"
)

type mfaUserCredentials struct {
	WebAuthnUserID []byte                `json:"webAuthnUserID"`
	TOTPSecretEnc  string                `json:"totpSecretEnc,omitempty"`
	TOTPEnabled    bool                  `json:"totpEnabled,omitempty"`
	Passkeys       []webauthn.Credential `json:"passkeys,omitempty"`
}

type mfaStoreFile struct {
	// MasterKey is a persistent AES-256 key (base64), independent of AuthCookieSigningKey.
	MasterKey string                         `json:"masterKey"`
	Users     map[string]*mfaUserCredentials `json:"users"`
}

type mfaCredentialStore struct {
	mu     sync.Mutex
	path   string
	encKey []byte
	users  map[string]*mfaUserCredentials
}

func newMFACredentialStore(path string, legacySigningKey []byte) (*mfaCredentialStore, error) {
	s := &mfaCredentialStore{
		path:  path,
		users: map[string]*mfaUserCredentials{},
	}

	var loadedMaster []byte

	if path != "" {
		master, err := s.load()
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		loadedMaster = master
	}

	if len(loadedMaster) == 32 {
		s.encKey = loadedMaster
	} else if len(s.users) == 0 {
		key, err := randomKey32()
		if err != nil {
			return nil, err
		}

		s.encKey = key
		if err := s.persist(); err != nil {
			return nil, err
		}
	} else {
		if err := s.migrateFromLegacySigningKey(legacySigningKey); err != nil {
			return nil, err
		}
	}

	if err := s.validateSecrets(); err != nil {
		return nil, err
	}

	return s, nil
}

func randomKey32() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, errors.Wrap(err, "unable to generate MFA master key")
	}

	return key, nil
}

func deriveMFAKey(signingKey []byte) ([]byte, error) {
	r := hkdf.New(sha256.New, signingKey, nil, []byte("kopia-ui-mfa-v1"))
	key := make([]byte, 32)

	if _, err := io.ReadFull(r, key); err != nil {
		return nil, errors.Wrap(err, "unable to derive MFA encryption key")
	}

	return key, nil
}

func (s *mfaCredentialStore) migrateFromLegacySigningKey(legacySigningKey []byte) error {
	legacyKey, err := deriveMFAKey(legacySigningKey)
	if err != nil {
		return err
	}

	newKey, err := randomKey32()
	if err != nil {
		return err
	}

	for username, u := range s.users {
		if u.TOTPSecretEnc == "" {
			continue
		}

		plain, err := decryptWithKey(legacyKey, u.TOTPSecretEnc)
		if err != nil {
			return errors.Wrapf(err, "unable to migrate TOTP secret for %q; restore KOPIA_AUTH_COOKIE_SIGNING_KEY or remove server-mfa.json", username)
		}

		enc, err := encryptWithKey(newKey, plain)
		if err != nil {
			return err
		}

		u.TOTPSecretEnc = enc
	}

	s.encKey = newKey

	return s.persist()
}

func (s *mfaCredentialStore) load() ([]byte, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}
	defer f.Close() //nolint:errcheck

	var doc mfaStoreFile
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		return nil, errors.Wrap(err, "invalid MFA credentials file")
	}

	if doc.Users == nil {
		doc.Users = map[string]*mfaUserCredentials{}
	}

	s.users = doc.Users

	if doc.MasterKey == "" {
		return nil, nil
	}

	key, err := base64.RawStdEncoding.DecodeString(doc.MasterKey)
	if err != nil {
		return nil, errors.Wrap(err, "invalid MFA master key")
	}

	if len(key) != 32 {
		return nil, errors.New("invalid MFA master key length")
	}

	return key, nil
}

func (s *mfaCredentialStore) persist() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.saveLocked()
}

func (s *mfaCredentialStore) saveLocked() error {
	if s.path == "" {
		return nil
	}

	doc := mfaStoreFile{
		MasterKey: base64.RawStdEncoding.EncodeToString(s.encKey),
		Users:     s.users,
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return errors.Wrap(err, "unable to marshal MFA credentials")
	}

	if err := atomic.WriteFile(s.path, bytes.NewReader(b)); err != nil {
		return errors.Wrap(err, "unable to write MFA credentials")
	}

	return nil
}

func (s *mfaCredentialStore) validateSecrets() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for username, u := range s.users {
		if !u.TOTPEnabled || u.TOTPSecretEnc == "" {
			continue
		}

		if _, err := decryptWithKey(s.encKey, u.TOTPSecretEnc); err != nil {
			return errors.Wrapf(err, "unable to decrypt TOTP secret for %q", username)
		}
	}

	return nil
}

func (s *mfaCredentialStore) getOrCreate(username string) (*mfaUserCredentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[username]
	if ok {
		return cloneMFAUser(u), nil
	}

	userID := make([]byte, 32)
	if _, err := rand.Read(userID); err != nil {
		return nil, errors.Wrap(err, "unable to generate WebAuthn user ID")
	}

	u = &mfaUserCredentials{WebAuthnUserID: userID}
	s.users[username] = u

	if err := s.saveLocked(); err != nil {
		return nil, err
	}

	return cloneMFAUser(u), nil
}

func (s *mfaCredentialStore) get(username string) *mfaUserCredentials {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[username]
	if !ok {
		return nil
	}

	return cloneMFAUser(u)
}

func (s *mfaCredentialStore) update(username string, mutate func(*mfaUserCredentials) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.users[username]
	if !ok {
		userID := make([]byte, 32)
		if _, err := rand.Read(userID); err != nil {
			return errors.Wrap(err, "unable to generate WebAuthn user ID")
		}

		u = &mfaUserCredentials{WebAuthnUserID: userID}
		s.users[username] = u
	}

	if err := mutate(u); err != nil {
		return err
	}

	return s.saveLocked()
}

func (s *mfaCredentialStore) encryptSecret(plaintext string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return encryptWithKey(s.encKey, plaintext)
}

func (s *mfaCredentialStore) decryptSecret(encoded string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return decryptWithKey(s.encKey, encoded)
}

func encryptWithKey(key []byte, plaintext string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", errors.Wrap(err, "aes")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", errors.Wrap(err, "gcm")
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", errors.Wrap(err, "nonce")
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.RawStdEncoding.EncodeToString(ciphertext), nil
}

func decryptWithKey(key []byte, encoded string) (string, error) {
	raw, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return "", errors.Wrap(err, "decode")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", errors.Wrap(err, "aes")
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", errors.Wrap(err, "gcm")
	}

	if len(raw) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}

	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]

	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", errors.Wrap(err, "decrypt")
	}

	return string(plain), nil
}

func (s *mfaCredentialStore) isTOTPEnabled(username string) bool {
	u := s.get(username)
	return u != nil && u.TOTPEnabled && u.TOTPSecretEnc != ""
}

func (s *mfaCredentialStore) hasPasskeys(username string) bool {
	u := s.get(username)
	return u != nil && len(u.Passkeys) > 0
}

func (s *mfaCredentialStore) totpSecret(username string) (string, bool) {
	u := s.get(username)
	if u == nil || !u.TOTPEnabled || u.TOTPSecretEnc == "" {
		return "", false
	}

	secret, err := s.decryptSecret(u.TOTPSecretEnc)
	if err != nil {
		return "", false
	}

	return secret, true
}

func (s *mfaCredentialStore) findByWebAuthnUserID(userHandle []byte) (string, *mfaUserCredentials) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for username, u := range s.users {
		if bytes.Equal(u.WebAuthnUserID, userHandle) {
			return username, cloneMFAUser(u)
		}
	}

	return "", nil
}

func cloneMFAUser(u *mfaUserCredentials) *mfaUserCredentials {
	if u == nil {
		return nil
	}

	cp := *u
	if u.WebAuthnUserID != nil {
		cp.WebAuthnUserID = append([]byte(nil), u.WebAuthnUserID...)
	}

	if u.Passkeys != nil {
		cp.Passkeys = make([]webauthn.Credential, len(u.Passkeys))
		copy(cp.Passkeys, u.Passkeys)
	}

	return &cp
}
