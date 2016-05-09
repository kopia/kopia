package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/blob"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/kopia/kopia/session"
	"github.com/kopia/kopia/vault"
)

var (
	traceStorage = app.Flag("trace-storage", "Enables tracing of storage operations.").Bool()

	vaultPath    = app.Flag("vault", "Specify the vault to use.").String()
	password     = app.Flag("password", "Vault password").String()
	passwordFile = app.Flag("passwordfile", "Read password from a file").ExistingFile()
	key          = app.Flag("key", "Vault key").String()
	keyFile      = app.Flag("keyfile", "Read vault key from a file").ExistingFile()
)

func failOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func mustOpenSession() session.Session {
	s, err := openSession()
	failOnError(err)
	return s
}

func getHomeDir() string {
	return os.Getenv("HOME")
}

func vaultConfigFileName() string {
	return filepath.Join(getHomeDir(), ".kopia/vault.config")
}

// func loadConfig() (*config.Config, error) {
// 	path := configFileName()
// 	if path == "" {
// 		return nil, fmt.Errorf("Cannot find config file. You may pass --config_file to specify config file location.")
// 	}

// 	var cfg config.Config

// 	//log.Printf("Loading config file from %v", path)
// 	f, err := os.Open(path)
// 	if err != nil {
// 		return nil, fmt.Errorf("Error opening configuration file: %v", err)
// 	}
// 	defer f.Close()

// 	err = cfg.Load(f)
// 	if err == nil {
// 		return &cfg, nil
// 	}

// 	return nil, fmt.Errorf("Error loading configuration file: %v", err)
// }

func openSession() (session.Session, error) {
	return nil, nil
	// cfg, err := loadConfig()
	// if err != nil {
	// 	return nil, err
	// }

	// storage, err := blob.NewStorage(cfg.Storage)
	// if err != nil {
	// 	return nil, err
	// }

	// if *traceStorage {
	// 	storage = blob.NewLoggingWrapper(storage)
	// }

	// return session.New(storage)
}

type vaultConfig struct {
	Storage blob.StorageConfiguration `json:"storage"`
	Key     []byte                    `json:"key,omitempty"`
}

func persistVaultConfig(v *vault.Vault) error {
	vc := vaultConfig{
		Storage: v.Storage.Configuration(),
		Key:     v.MasterKey,
	}

	f, err := os.Create(vaultConfigFileName())
	if err != nil {
		return err
	}
	defer f.Close()
	json.NewEncoder(f).Encode(vc)
	return nil
}

func getPersistedVaultConfig() *vaultConfig {
	var vc vaultConfig

	f, err := os.Open(vaultConfigFileName())
	if err == nil {
		err = json.NewDecoder(f).Decode(&vc)
		f.Close()
		if err != nil {
			return nil
		}
		return &vc
	}

	return nil
}

func openVault() (*vault.Vault, error) {
	vc := getPersistedVaultConfig()
	if vc != nil {
		storage, err := blob.NewStorage(vc.Storage)
		if err != nil {
			return nil, err
		}

		return vault.OpenWithKey(storage, vc.Key)
	}

	if *vaultPath == "" {
		return nil, fmt.Errorf("vault not connected, use --vault")
	}

	storage, err := blob.NewStorageFromURL(*vaultPath)
	if err != nil {
		return nil, err
	}

	masterKey, password, err := getKeyOrPassword(false)
	if err != nil {
		return nil, err
	}

	if masterKey != nil {
		return vault.OpenWithKey(storage, masterKey)
	} else {
		return vault.OpenWithPassword(storage, password)
	}
}

func getKeyOrPassword(isNew bool) ([]byte, string, error) {
	if *key != "" {
		k, err := hex.DecodeString(*key)
		if err != nil {
			return nil, "", fmt.Errorf("invalid key format: %v", err)
		}

		return k, "", nil
	}

	if *password != "" {
		return nil, strings.TrimSpace(*password), nil
	}

	if *keyFile != "" {
		key, err := ioutil.ReadFile(*keyFile)
		if err != nil {
			return nil, "", fmt.Errorf("unable to read key file: %v", err)
		}

		return key, "", nil
	}

	if *passwordFile != "" {
		f, err := ioutil.ReadFile(*passwordFile)
		if err != nil {
			return nil, "", fmt.Errorf("unable to read password file: %v", err)
		}

		return nil, strings.TrimSpace(string(f)), nil
	}

	if isNew {
		for {
			fmt.Printf("Enter password: ")
			p1, err := askPass()
			if err != nil {
				return nil, "", err
			}
			fmt.Println()
			fmt.Printf("Enter password again: ")
			p2, err := askPass()
			if err != nil {
				return nil, "", err
			}
			fmt.Println()
			if p1 != p2 {
				fmt.Println("Passwords don't match!")
			} else {
				return nil, p1, nil
			}
		}
	} else {
		fmt.Printf("Enter password: ")
		p1, err := askPass()
		if err != nil {
			return nil, "", err
		}
		fmt.Println()
		return nil, p1, nil
	}
}

func askPass() (string, error) {
	b, err := terminal.ReadPassword(0)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
