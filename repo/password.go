package repo

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/zalando/go-keyring"
)

// KeyRingEnabled enables password persistence uses OS-specific keyring.
var KeyRingEnabled = false

// GetPersistedPassword retrieves persisted password for a given repository config.
func GetPersistedPassword(ctx context.Context, configFile string) (string, bool) {
	if KeyRingEnabled {
		kr, err := keyring.Get(getKeyringItemID(configFile), keyringUsername(ctx))
		if err == nil {
			log(ctx).Debugf("password for %v retrieved from OS keyring", configFile)
			return kr, true
		}
	}

	b, err := ioutil.ReadFile(passwordFileName(configFile))
	if err == nil {
		s, err := base64.StdEncoding.DecodeString(string(b))
		if err == nil {
			log(ctx).Debugf("password for %v retrieved from password file", configFile)
			return string(s), true
		}
	}

	log(ctx).Debugf("could not find persisted password")

	return "", false
}

// persistPassword stores password for a given repository config.
func persistPassword(ctx context.Context, configFile, password string) error {
	if KeyRingEnabled {
		log(ctx).Debugf("saving password to OS keyring...")

		err := keyring.Set(getKeyringItemID(configFile), keyringUsername(ctx), password)
		if err == nil {
			log(ctx).Debugf("Saved password in OS keyring")
			return nil
		}

		return errors.Wrap(err, "error saving password in key ring")
	}

	fn := passwordFileName(configFile)
	log(ctx).Debugf("Saving password to file %v.", fn)

	return ioutil.WriteFile(fn, []byte(base64.StdEncoding.EncodeToString([]byte(password))), 0o600)
}

// deletePassword removes stored repository password.
func deletePassword(ctx context.Context, configFile string) {
	// delete from both keyring and a file
	if KeyRingEnabled {
		err := keyring.Delete(getKeyringItemID(configFile), keyringUsername(ctx))
		if err == nil {
			log(ctx).Infof("deleted repository password for %v.", configFile)
		} else if !errors.Is(err, keyring.ErrNotFound) {
			log(ctx).Errorf("unable to delete keyring item %v: %v", getKeyringItemID(configFile), err)
		}
	}

	_ = os.Remove(passwordFileName(configFile))
}

func getKeyringItemID(configFile string) string {
	h := sha256.New()
	io.WriteString(h, configFile) //nolint:errcheck

	return fmt.Sprintf("%v-%x", filepath.Base(configFile), h.Sum(nil)[0:8])
}

func passwordFileName(configFile string) string {
	return configFile + ".kopia-password"
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
