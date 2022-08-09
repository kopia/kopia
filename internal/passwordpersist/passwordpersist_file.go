package passwordpersist

import (
	"context"
	"encoding/base64"
	"os"

	"github.com/pkg/errors"
)

// File is a Strategy that persists the base64-encoded password in a file next to repository config file.
func File() Strategy {
	return filePasswordStorage{}
}

const passwordFileMode = 0o600

type filePasswordStorage struct{}

func (filePasswordStorage) GetPassword(ctx context.Context, configFile string) (string, error) {
	b, err := os.ReadFile(passwordFileName(configFile))
	if os.IsNotExist(err) {
		return "", ErrPasswordNotFound
	}

	if err != nil {
		return "", errors.Wrap(err, "error reading persisted password")
	}

	s, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return "", errors.Wrap(err, "error invalid persisted password")
	}

	log(ctx).Debugf("password for %v retrieved from password file", configFile)

	return string(s), nil
}

func (filePasswordStorage) PersistPassword(ctx context.Context, configFile, password string) error {
	fn := passwordFileName(configFile)
	log(ctx).Debugf("Saving password to file %v.", fn)

	//nolint:wrapcheck
	return os.WriteFile(fn, []byte(base64.StdEncoding.EncodeToString([]byte(password))), passwordFileMode)
}

func (filePasswordStorage) DeletePassword(ctx context.Context, configFile string) error {
	err := os.Remove(passwordFileName(configFile))
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "error deleting password file")
	}

	return nil
}

func passwordFileName(configFile string) string {
	return configFile + ".kopia-password"
}
