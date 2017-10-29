package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/storage"

	// Register well-known blob storage providers
	_ "github.com/kopia/kopia/storage/filesystem"
	_ "github.com/kopia/kopia/storage/gcs"
)

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	PersistCredentials bool
	CacheDirectory     string
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st storage.Storage, creds auth.Credentials, opt ConnectOptions) error {
	cip, ok := st.(storage.ConnectionInfoProvider)
	if !ok {
		return errors.New("repository does not support persisting configuration")
	}

	f, err := readFormaBlock(st)
	if err != nil {
		return err
	}

	masterKey, err := creds.GetMasterKey(f.SecurityOptions)
	if err != nil {
		return fmt.Errorf("can't get master key: %v", err)
	}

	cfg := &config.RepositoryConnectionInfo{
		ConnectionInfo: cip.ConnectionInfo(),
		Key:            masterKey,
	}

	var lc config.LocalConfig
	lc.Connection = cfg

	if !opt.PersistCredentials {
		lc.Connection.Key = nil
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(configFile), 0700); err != nil {
		return err
	}

	if err := ioutil.WriteFile(configFile, d, 0600); err != nil {
		return nil
	}

	// now verify that the repository can be opened with the provided config file.
	r, err := Open(ctx, configFile, &Options{
		CredentialsCallback: func() (auth.Credentials, error) {
			return creds, nil
		},
	})
	if err != nil {
		return err
	}

	r.Close()
	return nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(configFile string) error {
	_, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	return os.Remove(configFile)
}
