package vault

import (
	"crypto/sha256"
	"io/ioutil"
	"sync"

	"golang.org/x/crypto/pbkdf2"
)

// Credentials encapsulates user credentials.
type Credentials interface {
	Username() string
	PrivateKey() *UserPrivateKey
}

type credentials struct {
	sync.Mutex
	once sync.Once

	username       string
	privateKey     *UserPrivateKey
	passwordPrompt func() string
}

func (pc *credentials) Username() string {
	return pc.username
}

func (pc *credentials) PrivateKey() *UserPrivateKey {
	pc.once.Do(pc.deriveKeyFromPassword)

	return pc.privateKey
}

func (pc *credentials) deriveKeyFromPassword() {
	if pc.privateKey != nil {
		return
	}

	password := pc.passwordPrompt()
	k := pbkdf2.Key([]byte(password), []byte(pc.username), pbkdf2Rounds, 32, sha256.New)
	pk, err := newPrivateKey(k)
	if err != nil {
		panic("should not happen")
	}
	pc.privateKey = pk
}

// Password returns Credentials object with static username and password.
func Password(username, password string) Credentials {
	return &credentials{
		username: username,
		passwordPrompt: func() string {
			return password
		},
	}
}

// PasswordPrompt returns Credentials object that will prompt user for password using the specified callback function.
func PasswordPrompt(username string, prompt func() string) Credentials {
	return &credentials{
		username:       username,
		passwordPrompt: prompt,
	}
}

// Key returns Credentials object with specified username and key bytes.
func Key(username string, key []byte) (Credentials, error) {
	pk, err := newPrivateKey(key)
	if err != nil {
		return nil, err
	}

	return &credentials{
		username:   username,
		privateKey: pk,
	}, nil
}

// KeyFromFile returns Credentials object with specified username and with key read from the specified file.
func KeyFromFile(username string, fileName string) (Credentials, error) {
	k, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	return Key(username, k)
}
