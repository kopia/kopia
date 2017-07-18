package cli

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bgentry/speakeasy"
	"github.com/kopia/kopia/blob/logging"
	"github.com/kopia/kopia/client"
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vault"
)

var (
	traceStorage = app.Flag("trace-storage", "Enables tracing of storage operations.").Hidden().Envar("KOPIA_TRACE_STORAGE").Bool()
	traceLocalFS = app.Flag("trace-localfs", "Enables tracing of local filesystem operations").Hidden().Envar("KOPIA_TRACE_STORAGE").Bool()

	vaultConfigPath = app.Flag("vaultconfig", "Specify the vault config file to use.").PlaceHolder("PATH").Envar("KOPIA_VAULTCONFIG").String()
	vaultPath       = app.Flag("vault", "Specify the vault to use.").PlaceHolder("PATH").Envar("KOPIA_VAULT").Short('v').String()
	password        = app.Flag("password", "Vault password.").Envar("KOPIA_PASSWORD").Short('p').String()
	passwordFile    = app.Flag("passwordfile", "Read vault password from a file.").PlaceHolder("FILENAME").Envar("KOPIA_PASSWORD_FILE").ExistingFile()
	key             = app.Flag("key", "Specify vault master key (hexadecimal).").Envar("KOPIA_KEY").Short('k').String()
	keyFile         = app.Flag("keyfile", "Read vault master key from file.").PlaceHolder("FILENAME").Envar("KOPIA_KEY_FILE").ExistingFile()

	maxDownloadSpeed = app.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").Int()
	maxUploadSpeed   = app.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").Int()
)

func failOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func getContext() context.Context {
	ctx := context.Background()
	return ctx
}

func openConnection(options ...repo.RepositoryOption) (*client.Connection, error) {
	return client.Open(getContext(), vaultConfigFileName(), connectionOptionsFromFlags(options...))
}

func connectionOptionsFromFlags(options ...repo.RepositoryOption) *client.Options {
	opts := &client.Options{
		CredentialsCallback: func() (vault.Credentials, error) { return getVaultCredentials(false) },
		RepositoryOptions:   options,
	}

	if *traceStorage {
		opts.TraceStorage = log.Printf
	}

	if *maxUploadSpeed != 0 {
		opts.MaxUploadSpeed = *maxUploadSpeed
	}

	if *maxDownloadSpeed != 0 {
		opts.MaxDownloadSpeed = *maxDownloadSpeed
	}

	return opts
}

func mustOpenConnection(repoOptions ...repo.RepositoryOption) *client.Connection {
	s, err := openConnection(repoOptions...)
	failOnError(err)
	return s
}

func repositoryOptionsFromFlags(extraOptions []repo.RepositoryOption) []repo.RepositoryOption {
	var opts []repo.RepositoryOption

	for _, o := range extraOptions {
		opts = append(opts, o)
	}

	if *traceStorage {
		opts = append(opts, repo.EnableLogging(logging.Prefix("[REPOSITORY] ")))
	}
	return opts
}

func getHomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}

	return os.Getenv("HOME")
}

func vaultConfigFileName() string {
	if len(*vaultConfigPath) > 0 {
		return *vaultConfigPath
	}
	return filepath.Join(getHomeDir(), ".kopia/vault.config")
}

func persistVaultConfig(v *vault.Vault) error {
	cfg, err := v.Config()
	if err != nil {
		return err
	}

	var lc config.LocalConfig
	lc.VaultConnection = cfg

	fname := vaultConfigFileName()
	log.Printf("Saving vault configuration to '%v'.", fname)
	if err := os.MkdirAll(filepath.Dir(fname), 0700); err != nil {
		return err
	}

	d, err := json.MarshalIndent(&lc, "", "  ")
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fname, d, 0600)
}

func openVaultSpecifiedByFlag() (*vault.Vault, error) {
	if *vaultPath == "" {
		return nil, fmt.Errorf("--vault must be specified")
	}
	storage, err := newStorageFromURL(getContext(), *vaultPath)
	if err != nil {
		return nil, err
	}

	creds, err := getVaultCredentials(false)
	if err != nil {
		return nil, err
	}

	return vault.Open(storage, creds)
}

func getVaultCredentials(isNew bool) (vault.Credentials, error) {
	if *key != "" {
		k, err := hex.DecodeString(*key)
		if err != nil {
			return nil, fmt.Errorf("invalid key format: %v", err)
		}

		return vault.MasterKey(k)
	}

	if *password != "" {
		return vault.Password(strings.TrimSpace(*password))
	}

	if *keyFile != "" {
		key, err := ioutil.ReadFile(*keyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read key file: %v", err)
		}

		return vault.MasterKey(key)
	}

	if *passwordFile != "" {
		f, err := ioutil.ReadFile(*passwordFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read password file: %v", err)
		}

		return vault.Password(strings.TrimSpace(string(f)))
	}
	if isNew {
		for {
			p1, err := askPass("Enter password to create new vault: ")
			if err != nil {
				return nil, err
			}
			p2, err := askPass("Re-enter password for verification: ")
			if err != nil {
				return nil, err
			}
			if p1 != p2 {
				fmt.Println("Passwords don't match!")
			} else {
				return vault.Password(p1)
			}
		}
	} else {
		p1, err := askPass("Enter password to open vault: ")
		if err != nil {
			return nil, err
		}
		fmt.Println()
		return vault.Password(p1)
	}
}

func mustGetLocalFSEntry(path string) fs.Entry {
	e, err := localfs.NewEntry(path, nil)
	if err == nil {
		failOnError(err)
	}

	if *traceLocalFS {
		return loggingfs.Wrap(e, loggingfs.Prefix("[LOCALFS] "))
	}

	return e
}

func askPass(prompt string) (string, error) {
	for {
		b, err := speakeasy.Ask(prompt)
		if err != nil {
			return "", err
		}

		p := string(b)

		if len(p) == 0 {
			continue
		}

		if len(p) >= vault.MinPasswordLength {
			return p, nil
		}

		fmt.Printf("Password too short, must be at least %v characters, you entered %v. Try again.", vault.MinPasswordLength, len(p))
		fmt.Println()
	}
}
