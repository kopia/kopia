package passwordpersist

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/byteness/keyring"
	"github.com/pkg/errors"
)

// Keyring is a Strategy that persists the password in OS-specific keyring.
func Keyring() Strategy {
	return keyringStrategy{}
}

type keyringStrategy struct{}

func (keyringStrategy) GetPassword(ctx context.Context, configFile string) (string, error) {
	kr, err := keyring.Open(getKeyringConfig(configFile))
	if err != nil {
		return "", errors.Wrap(err, "error accessing OS keyring, the keyring may be locked, attempt unlocking it using the OS-specific method")
	}

	item, err := kr.Get(keyringUsername(ctx))
	switch {
	case err == nil:
		log(ctx).Debugf("password for %v retrieved from OS keyring", configFile)
		return string(item.Data), nil
	case errors.Is(err, keyring.ErrKeyNotFound):
		return "", ErrPasswordNotFound
	default:
		return "", errors.Wrap(err, "error retrieving password from OS keyring, the keyring may be locked, attempt unlocking it using the OS-specific method")
	}
}

func (keyringStrategy) PersistPassword(ctx context.Context, configFile, password string) error {
	log(ctx).Debug("saving password to OS keyring...")

	kr, err := keyring.Open(getKeyringConfig(configFile))
	if err != nil {
		return errors.Wrap(err, "error accessing OS keyring, the keyring may be locked, attempt unlocking it using the OS-specific method")
	}

	if err := kr.Set(keyring.Item{
		Key:  keyringUsername(ctx),
		Data: []byte(password),
	}); err != nil {
		return errors.Wrap(err, "error saving password in OS keyring")
	}

	log(ctx).Debug("Saved password in OS keyring")
	return nil
}

func (keyringStrategy) DeletePassword(ctx context.Context, configFile string) error {
	kr, err := keyring.Open(getKeyringConfig(configFile))
	if err != nil {
		return errors.Wrap(err, "error accessing OS keyring, the keyring may be locked, attempt unlocking it using the OS-specific method")
	}

	if err := kr.Remove(keyringUsername(ctx)); err != nil {
		return errors.Wrapf(err, "unable to delete keyring item %v", getKeyringItemID(configFile))
	}

	log(ctx).Infof("deleted repository password for %v.", configFile)
	return nil
}

func getKeyringConfig(configFile string) keyring.Config {
	return keyring.Config{
		ServiceName:              getKeyringItemID(configFile),
		KeychainTrustApplication: true,
		AllowedBackends: []keyring.BackendType{
			keyring.WinCredBackend,
			keyring.KeychainBackend,
			keyring.SecretServiceBackend,
		},
	}
}

func getKeyringItemID(configFile string) string {
	h := sha256.New()
	io.WriteString(h, configFile) //nolint:errcheck

	return fmt.Sprintf("%v-%x", filepath.Base(configFile), h.Sum(nil)[0:8])
}

func keyringUsername(ctx context.Context) string {
	currentUser, err := user.Current()
	if err != nil {
		log(ctx).Errorf("Cannot determine keyring username: %s", err)
		return "nobody"
	}

	u := currentUser.Username

	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u
}
