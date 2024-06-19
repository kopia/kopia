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

	"github.com/pkg/errors"
	"github.com/zalando/go-keyring"
)

// Keyring is a Strategy that persists the password in OS-specific keyring.
func Keyring() Strategy {
	return keyringStrategy{}
}

type keyringStrategy struct{}

func (keyringStrategy) GetPassword(ctx context.Context, configFile string) (string, error) {
	kr, err := keyring.Get(getKeyringItemID(configFile), keyringUsername(ctx))

	switch {
	case err == nil:
		log(ctx).Debugf("password for %v retrieved from OS keyring", configFile)
		return kr, nil
	case errors.Is(err, keyring.ErrNotFound):
		return "", ErrPasswordNotFound
	case errors.Is(err, keyring.ErrUnsupportedPlatform):
		return "", ErrPasswordNotFound
	default:
		return "", errors.Wrap(err, "error retrieving password from OS keyring")
	}
}

func (keyringStrategy) PersistPassword(ctx context.Context, configFile, password string) error {
	log(ctx).Debug("saving password to OS keyring...")

	err := keyring.Set(getKeyringItemID(configFile), keyringUsername(ctx), password)

	switch {
	case err == nil:
		log(ctx).Debug("Saved password in OS keyring")
		return nil

	case errors.Is(err, keyring.ErrUnsupportedPlatform):
		return ErrUnsupported

	default:
		return errors.Wrap(err, "error saving password in OS keyring")
	}
}

func (keyringStrategy) DeletePassword(ctx context.Context, configFile string) error {
	err := keyring.Delete(getKeyringItemID(configFile), keyringUsername(ctx))

	switch {
	case err == nil:
		log(ctx).Infof("deleted repository password for %v.", configFile)
		return nil

	case errors.Is(err, keyring.ErrUnsupportedPlatform):
		return ErrUnsupported

	case errors.Is(err, keyring.ErrNotFound):
		return ErrPasswordNotFound

	default:
		return errors.Wrapf(err, "unable to delete keyring item %v", getKeyringItemID(configFile))
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
