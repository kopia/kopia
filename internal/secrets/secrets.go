package secrets

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/zalando/go-keyring"
)

type keyType string

// Secret types.
const (
	Unset   keyType = ""
	Command keyType = "command:"
	Config  keyType = "config:"
	EnvVar  keyType = "envvar:"
	File    keyType = "file:"
	Keyring keyType = "keyring:"
	Value   keyType = "plaintext:"
	Vault   keyType = "vault:"
)

// Secret holds secrets.
//
//nolint:musttag
type Secret struct {
	Input      string
	Value      string
	StoreValue string
	Type       keyType
}

type encryptedValue struct {
	Value string `json:"encrypted"`
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
	case strings.HasPrefix(value, string(EnvVar)):
		s.Type = EnvVar
		s.Input = value[len(EnvVar):]
	case strings.HasPrefix(value, string(Command)):
		s.Type = Command
		s.Input = value[len(Command):]
	case strings.HasPrefix(value, string(Keyring)):
		s.Type = Keyring
		s.Input = value[len(Keyring):]
	case strings.HasPrefix(value, string(Vault)):
		s.Type = Vault
		s.Input = value[len(Vault):]
	case strings.HasPrefix(value, string(File)):
		s.Type = File
		s.Input = value[len(File):]
	case strings.HasPrefix(value, string(Value)):
		s.Type = Value
		s.Value = value[len(Value):]
		s.Input = s.Value
	case value == "":
		s.Type = Unset
		s.Input = ""
		s.Value = ""
	default:
		s.Type = Value
		s.Value = value
		s.Input = value
	}

	return nil
}

// IsSet returns whether a secret has been configured.
func (s *Secret) IsSet() bool {
	if s == nil {
		return false
	}

	switch s.Type {
	case Unset:
		return false
	case EnvVar:
		_, isset := os.LookupEnv(s.Input)
		return isset
	default:
		return true
	}
}

// String will return the decoded version of the Secret.
func (s *Secret) String() string {
	if s == nil {
		return ""
	}

	return s.Value
}

// Bytes will return the decoded version of the Secret.
func (s *Secret) Bytes() []byte {
	if s == nil || s.Value == "" {
		return nil
	}

	return []byte(s.Value)
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
		err = s.evaluateEnvVar()
	case File:
		err = s.evaluateFile()
	case Command:
		err = s.evaluateCommand()
	case Keyring:
		err = s.evaluateKeyring()
	case Vault:
		err = errors.New("Vault keys are not yet supported")
	default:
		return nil
	}

	if err != nil {
		s.Value = ""
	} else {
		s.Value = strings.TrimSpace(s.Value)
	}

	return err
}

func (s *Secret) evaluateEnvVar() error {
	s.Value = os.Getenv(s.Input)
	if s.Value == "" {
		return errors.New("Failed to find env variable")
	}

	return nil
}

func (s *Secret) evaluateFile() error {
	var body []byte

	var err error

	body, err = os.ReadFile(s.Input)
	if err != nil {
		return errors.Wrapf(err, "failed toread file: %v", s.Input)
	}

	s.Value = string(body)

	return nil
}

func (s *Secret) evaluateCommand() error {
	var res []byte

	var err error

	cmdParts := strings.Fields(s.Input)
	cmd := cmdParts[0]
	args := cmdParts[1:]

	res, err = exec.Command(cmd, args...).Output() //nolint:gosec
	if err != nil {
		return errors.Wrapf(err, "Command failed: %v", s.Input)
	}

	s.Value = string(res)

	return nil
}

func (s *Secret) evaluateKeyring() error {
	var username string

	var err error

	var kr string

	username, err = keyringUsername()
	if err == nil {
		kr, err = keyring.Get(s.Input, username)
		if err == nil {
			s.Value = kr
			return nil
		}
	}

	return errors.Wrap(err, "failed to read from keyring")
}

// MarshalJSON will emit an encrypted secret if the original type was Config or Value else "".
func (s Secret) MarshalJSON() ([]byte, error) {
	if s.Type == Value || s.Type == Config {
		encrypted := encryptedValue{Value: s.StoreValue}
		//nolint:wrapcheck
		return json.Marshal(encrypted)
	}

	//nolint:wrapcheck
	return json.Marshal("")
}

// UnmarshalJSON parses octal permissions string from JSON.
func (s *Secret) UnmarshalJSON(b []byte) error {
	if b == nil || string(b) == "\"\"" {
		return nil
	}

	var encrypted encryptedValue

	var unencrypted string

	if err := json.Unmarshal(b, &encrypted); err != nil {
		if err = json.Unmarshal(b, &unencrypted); err != nil {
			return errors.Wrap(err, "Failed to unmarshal secret")
		}

		s.Value = unencrypted
		s.Type = Value

		return nil
	}

	s.StoreValue = encrypted.Value
	s.Type = Config

	return nil
}

// decrypt a Secret with the signing key and password.
func (s *Secret) decrypt(signingKey *EncryptedToken, encrypted, password string) (string, error) {
	encbytes, err := hex.DecodeString(encrypted)
	if err != nil {
		return "", errors.Wrap(err, "Failed to decode hex password")
	}

	data, err := signingKey.Decrypt(encbytes, password)
	if err != nil {
		return "", errors.Wrap(err, "Failed to decrypt data")
	}

	return string(data), nil
}

// encrypt a Secret with the signing key and password.
func (s *Secret) encrypt(signingKey *EncryptedToken, decrypted, password string) (string, error) {
	// Encrypt data with sigining key
	data, err := signingKey.Encrypt([]byte(decrypted), password)
	if err != nil {
		return "", errors.Wrap(err, "Failed to encrypt the secret")
	}

	// Return hexified version of encrypted data
	return hex.EncodeToString(data), nil
}

func keyringUsername() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", errors.Errorf("Cannot determine keyring username: %s", err)
	}

	u := currentUser.Username

	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u, nil
}
