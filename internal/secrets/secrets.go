package secrets

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/fernet/fernet-go"
	"github.com/pkg/errors"
)

type keyType int

// Secret types.
const (
	Unset keyType = iota
	Config
	EnvVar
	Command
	Value
	Keychain
	Vault
)

// Secret holds secrets.
type Secret struct {
	Input      string
	Value      string
	StoreValue string
	Type       keyType
}

// NewSecret constructs a new Secret.
func NewSecret(value string) *Secret {
	s := Secret{}

	err := s.Set(value)
	if err != nil {
		return nil
	}

	return &s
}

// Set will set the Secret type.
func (s *Secret) Set(value string) error {
	switch {
	case strings.HasPrefix(value, "envvar:"):
		s.Type = EnvVar
		s.Input = value[len("envvar:"):]
	case strings.HasPrefix(value, "command:"):
		s.Type = Command
		s.Input = value[len("command:"):]
	case strings.HasPrefix(value, "keychain:"):
		s.Type = Keychain
		s.Input = value[len("keychain:"):]
	case strings.HasPrefix(value, "vault:"):
		s.Type = Vault
		s.Input = value[len("vault:"):]
	case strings.HasPrefix(value, "plaintext:"):
		s.Type = Value
		s.Value = value[len("plaintext:"):]
		s.Input = s.Value
	default:
		s.Type = Value
		s.Value = value
		s.Input = value
	}

	return nil
}

// IsSet returns whether a secret has been configured.
func (s *Secret) IsSet() bool {
	return s.Type != Unset
}

// String will return the decoded version of the Secret.
func (s *Secret) String() string {
	return s.Value
}

// Evaluate a secret to fill all fields.
func (s *Secret) Evaluate(encryptedToken *EncryptedToken, password string) error {
	var err error

	switch s.Type {
	case Config:
		s.Value, err = s.decrypt(encryptedToken, s.StoreValue, password)
	case Value:
		if encryptedToken != nil && password != "" {
			s.StoreValue, err = s.encrypt(encryptedToken, s.Input, password)
		}
	case EnvVar:
		s.Value = os.Getenv(s.Input)
		if s.Value == "" {
			err = errors.New("Failed to find env variable")
		}
	case Command:
		var res []byte

		cmdParts := strings.Fields(s.Input)
		cmd := cmdParts[0]
		args := cmdParts[1:]

		res, err = exec.Command(cmd, args...).Output() //nolint:gosec
		if err != nil {
			s.Value = string(res)
		}
	case Vault:
		err = errors.New("Vault keys are not yet supported")
	case Keychain:
		err = errors.New("Keychain keys are not yet supported")
	default:
		return nil
	}

	return err
}

// MarshalJSON will emit an encrypted secret if the original type was Config or Value else "".
func (s Secret) MarshalJSON() ([]byte, error) {
	if s.Type == Value || s.Type == Config {
		//nolint:wrapcheck
		return json.Marshal(s.StoreValue)
	}

	//nolint:wrapcheck
	return json.Marshal("")
}

// UnmarshalJSON parses octal permissions string from JSON.
func (s *Secret) UnmarshalJSON(b []byte) error {
	if b == nil {
		return nil
	}

	var d string

	if err := json.Unmarshal(b, &d); err != nil {
		return errors.Wrap(err, "Failed to unmarshal secret")
	}

	s.StoreValue = d
	s.Type = Config

	return nil
}

// SecretVar is called by kingpin to handle Secret arguments.
func SecretVar(s kingpin.Settings, target **Secret) {
	if *target == nil {
		secret := Secret{}
		*target = &secret
	}

	s.SetValue(*target)
}

// SecretVarWithEnv is called by kingpin to handle Secret arguments with a default environment variable.
// Use this instead of kingpin's EnvName because it provides no limitations on the password value.
func SecretVarWithEnv(s kingpin.Settings, envvar string, target **Secret) {
	if *target == nil {
		secret := Secret{}
		*target = &secret
	}

	(*target).Type = EnvVar
	(*target).Input = envvar

	s.SetValue(*target)
}

// decrypt a Secret with the signing key and password.
func (s *Secret) decrypt(encryptedToken *EncryptedToken, encrypted, password string) (string, error) {
	// Convert EncryptedToken + pasword to sigining key
	key, err := encryptedToken.signingKey(password)
	if err != nil {
		return "", errors.Wrap(err, "Failed to get signing key")
	}

	encbytes, err := hex.DecodeString(encrypted)
	if err != nil {
		return "", errors.Wrap(err, "Failed to decode hex password")
	}

	data := fernet.VerifyAndDecrypt(encbytes, 0, []*fernet.Key{key})
	if data == nil {
		return "", errors.New("Failed to decrypt data")
	}

	return string(data), nil
}

// encrypt a Secret with the signing key and password.
func (s *Secret) encrypt(encryptedToken *EncryptedToken, decrypted, password string) (string, error) {
	// Convert EncryptedToken + pasword to sigining key
	key, err := encryptedToken.signingKey(password)
	if err != nil {
		return "", errors.Wrap(err, "Failed to get signing key")
	}

	// Encrypt data with sigining key
	data, err := fernet.EncryptAndSign([]byte(decrypted), key)
	if err != nil {
		return "", errors.Wrap(err, "Failed to encrypt the signining key")
	}

	// Return hexified version of encrypted data
	return hex.EncodeToString(data), nil
}
