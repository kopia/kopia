package repo

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/caching"
	"github.com/kopia/kopia/blob/logging"

	// Register well-known blob storage providers
	_ "github.com/kopia/kopia/blob/filesystem"
	_ "github.com/kopia/kopia/blob/gcs"
)

// Connect connects to the Repository specified in the specified configuration file.
func Connect(ctx context.Context, configFile string, options *ConnectOptions) (*Repository, error) {
	lc, err := LoadFromFile(configFile)
	if err != nil {
		return nil, err
	}

	var creds auth.Credentials
	if len(lc.VaultConnection.Key) > 0 {
		creds, err = auth.MasterKey(lc.VaultConnection.Key)
	} else {
		if options.CredentialsCallback == nil {
			return nil, errors.New("vault key not persisted and no credentials specified")
		}
		creds, err = options.CredentialsCallback()
	}

	if err != nil {
		return nil, fmt.Errorf("invalid vault credentials: %v", err)
	}

	rawVaultStorage, err := newStorageWithOptions(ctx, lc.VaultConnection.ConnectionInfo, options)
	if err != nil {
		return nil, fmt.Errorf("cannot open vault storage: %v", err)
	}

	vaultStorage := rawVaultStorage

	if options.TraceStorage != nil {
		vaultStorage = logging.NewWrapper(vaultStorage, logging.Prefix("[VAULT] "), logging.Output(options.TraceStorage))
	}

	vlt, err := Open(vaultStorage, creds)
	if err != nil {
		rawVaultStorage.Close()
		return nil, fmt.Errorf("unable to open vault: %v", err)
	}

	repositoryStorage := rawVaultStorage
	if options.TraceStorage != nil {
		repositoryStorage = logging.NewWrapper(repositoryStorage, logging.Prefix("[STORAGE] "), logging.Output(options.TraceStorage))
	}

	if lc.Caching != nil {
		rs, err := caching.NewWrapper(ctx, repositoryStorage, lc.Caching)
		if err != nil {
			vaultStorage.Close()
			repositoryStorage.Close()
			return nil, err
		}
		repositoryStorage = rs
		if options.TraceStorage != nil {
			repositoryStorage = logging.NewWrapper(repositoryStorage, logging.Prefix("[CACHE] "), logging.Output(options.TraceStorage))
		}
	}

	r, err := NewRepository(repositoryStorage, vlt.RepoConfig.Format)
	if err != nil {
		vaultStorage.Close()
		repositoryStorage.Close()
		return nil, err
	}
	r.Vault = vlt
	return r, nil
}

func newStorageWithOptions(ctx context.Context, cfg blob.ConnectionInfo, options *ConnectOptions) (blob.Storage, error) {
	s, err := blob.NewStorage(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if options.MaxUploadSpeed > 0 || options.MaxDownloadSpeed > 0 {
		t, ok := s.(blob.Throttler)
		if ok {
			t.SetThrottle(options.MaxDownloadSpeed, options.MaxUploadSpeed)
		} else {
			log.Printf("Throttling not supported for '%v'.", cfg.Type)
		}
	}

	return s, nil
}
