package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/blob"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/kopia/kopia/vault"
)

var (
	traceStorage = app.Flag("trace-storage", "Enables tracing of storage operations.").Hidden().Bool()

	vaultPath    = app.Flag("vault", "Specify the vault to use.").Envar("KOPIA_VAULT").String()
	password     = app.Flag("password", "Vault password.").Envar("KOPIA_PASSWORD").String()
	passwordFile = app.Flag("passwordfile", "Read vault password from a file.").Envar("KOPIA_PASSWORD_FILE").ExistingFile()
	key          = app.Flag("key", "Specify vault master key (hexadecimal).").Envar("KOPIA_KEY").String()
	keyFile      = app.Flag("keyfile", "Read vault master key from file.").Envar("KOPIA_KEY_FILE").ExistingFile()
)

func failOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func mustOpenVault() *vault.Vault {
	s, err := openVault()
	failOnError(err)
	return s
}

func getHomeDir() string {
	return os.Getenv("HOME")
}

func vaultConfigFileName() string {
	return filepath.Join(getHomeDir(), ".kopia/vault.config")
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

	b, err := json.MarshalIndent(&vc, "", "  ")
	if err != nil {
		return err
	}

	_, err = f.Write(b)

	return err
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
		return nil, fmt.Errorf("vault not connected and not specified, use --vault or run 'kopia connect'")
	}

	return openVaultSpecifiedByFlag()
}

func openVaultSpecifiedByFlag() (*vault.Vault, error) {
	if *vaultPath == "" {
		return nil, fmt.Errorf("--vault must be specified")
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
	}

	return vault.OpenWithPassword(storage, password)
}

var errPasswordTooShort = errors.New("password too short")

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
			fmt.Printf("Enter password to create new vault: ")
			p1, err := askPass()
			fmt.Println()
			if err == errPasswordTooShort {
				fmt.Printf("Password too short, must be at least %v characters, you entered %v. Try again.", vault.MinPasswordLength, len(p1))
				fmt.Println()
				continue
			}
			if err != nil {
				return nil, "", err
			}
			fmt.Printf("Re-enter password for verification: ")
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
		fmt.Printf("Enter password to open vault: ")
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

	p := string(b)

	if len(p) < vault.MinPasswordLength {
		return p, errPasswordTooShort
	}

	return p, nil
}
