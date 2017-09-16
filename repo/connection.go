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
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/logging"
	"github.com/kopia/kopia/internal/config"

	// Register well-known blob storage providers
	_ "github.com/kopia/kopia/blob/filesystem"
	_ "github.com/kopia/kopia/blob/gcs"
)

// Options provides configuration parameters for connection to a repository.
type Options struct {
	CredentialsCallback func() (auth.Credentials, error)    // Provides credentials required to open the repository if not persisted.
	TraceStorage        func(f string, args ...interface{}) // Logs all storage access using provided Printf-style function
	TraceObjectManager  func(f string, args ...interface{}) // Logs all object manager activity using provided Printf-style function
	WriteBack           int                                 // Causes all object writes to be asynchronous with the specified number of workers.
}

// Open opens a Repository specified in the configuration file.
func Open(ctx context.Context, configFile string, options *Options) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}

	configFile, err := filepath.Abs(configFile)
	if err != nil {
		return nil, err
	}

	lc, err := config.LoadFromFile(configFile)
	if err != nil {
		return nil, err
	}

	var creds auth.Credentials
	if len(lc.Connection.Key) > 0 {
		creds, err = auth.MasterKey(lc.Connection.Key)
	} else {
		if options.CredentialsCallback == nil {
			return nil, errors.New("key not persisted and no credentials specified")
		}
		creds, err = options.CredentialsCallback()
	}

	if err != nil {
		return nil, fmt.Errorf("invalid credentials: %v", err)
	}

	st, err := blob.NewStorage(ctx, lc.Connection.ConnectionInfo)
	if err != nil {
		return nil, fmt.Errorf("cannot open storage: %v", err)
	}

	r, err := connect(ctx, st, creds, options)
	if err != nil {
		st.Close()
		return nil, err
	}

	r.ConfigFile = configFile
	r.CacheDirectory = applyDefaultString(lc.CacheDirectory, filepath.Join(filepath.Dir(configFile), "cache"))

	return r, nil
}

// ConnectOptions specifies options when persisting configuration to connect to a repository.
type ConnectOptions struct {
	PersistCredentials bool
	CacheDirectory     string
}

// Connect connects to the repository in the specified storage and persists the configuration and credentials in the file provided.
func Connect(ctx context.Context, configFile string, st blob.Storage, creds auth.Credentials, opt ConnectOptions) error {
	r, err := connect(ctx, st, creds, nil)
	if err != nil {
		return err
	}

	cfg, err := r.Metadata.connectionConfiguration()
	if err != nil {
		return err
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

	return ioutil.WriteFile(configFile, d, 0600)
}

func connect(ctx context.Context, st blob.Storage, creds auth.Credentials, options *Options) (*Repository, error) {
	if options == nil {
		options = &Options{}
	}
	if options.TraceStorage != nil {
		st = logging.NewWrapper(st, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	mm, err := newMetadataManager(st, creds)
	if err != nil {
		return nil, fmt.Errorf("unable to open metadata manager: %v", err)
	}

	om, err := newObjectManager(st, mm.repoConfig.Format, options)
	if err != nil {
		return nil, fmt.Errorf("unable to open object manager: %v", err)
	}

	return &Repository{
		Objects:  om,
		Metadata: mm,
		Storage:  st,
	}, nil
}

// Disconnect removes the specified configuration file and any local cache directories.
func Disconnect(configFile string) error {
	_, err := config.LoadFromFile(configFile)
	if err != nil {
		return err
	}

	return os.Remove(configFile)
}
