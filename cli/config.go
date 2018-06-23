package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/bgentry/speakeasy"
	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo"
)

var (
	traceStorage       = app.Flag("trace-storage", "Enables tracing of storage operations.").Default("true").Hidden().Bool()
	traceObjectManager = app.Flag("trace-object-manager", "Enables tracing of object manager operations.").Envar("KOPIA_TRACE_OBJECT_MANAGER").Bool()
	traceLocalFS       = app.Flag("trace-localfs", "Enables tracing of local filesystem operations").Envar("KOPIA_TRACE_FS").Bool()
	enableCaching      = app.Flag("caching", "Enables caching of objects (disable with --no-caching)").Default("true").Hidden().Bool()
	enableListCaching  = app.Flag("list-caching", "Enables caching of list results (disable with --no-list-caching)").Default("true").Hidden().Bool()

	configPath   = app.Flag("config-file", "Specify the config file to use.").Default(defaultConfigFileName()).Envar("KOPIA_CONFIG_PATH").String()
	password     = app.Flag("password", "Repository password.").Envar("KOPIA_PASSWORD").Short('p').String()
	passwordFile = app.Flag("passwordfile", "Read repository password from a file.").PlaceHolder("FILENAME").Envar("KOPIA_PASSWORD_FILE").ExistingFile()
	key          = app.Flag("key", "Specify master key (hexadecimal).").Envar("KOPIA_KEY").Short('k').String()
	keyFile      = app.Flag("keyfile", "Read master key from file.").PlaceHolder("FILENAME").Envar("KOPIA_KEY_FILE").ExistingFile()
)

func failOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func onCtrlC(f func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		f()
	}()
}

func waitForCtrlC() {
	// Wait until ctrl-c pressed
	done := make(chan bool)
	onCtrlC(func() {
		if done != nil {
			close(done)
			done = nil
		}
	})
	<-done
}

func openRepository(ctx context.Context, opts *repo.Options) (*repo.Repository, error) {
	r, err := repo.Open(ctx, repositoryConfigFileName(), applyOptionsFromFlags(opts))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("not connected to a repository, use 'kopia connect'")
	}

	return r, err
}

func applyOptionsFromFlags(opts *repo.Options) *repo.Options {
	if opts == nil {
		opts = &repo.Options{}
	}
	opts.CredentialsCallback = func() (auth.Credentials, error) { return getRepositoryCredentials(false) }

	if *traceStorage {
		opts.TraceStorage = log.Printf
	}

	if *traceObjectManager {
		opts.ObjectManagerOptions.Trace = log.Printf
	}

	return opts
}

func mustOpenRepository(ctx context.Context, opts *repo.Options) *repo.Repository {
	s, err := openRepository(ctx, opts)
	failOnError(err)
	return s
}

func repositoryConfigFileName() string {
	return *configPath
}

func defaultConfigFileName() string {
	return filepath.Join(ospath.ConfigDir(), "repository.config")
}

func getRepositoryCredentialsFromMasterKey() (auth.Credentials, error) {
	k, err := hex.DecodeString(*key)
	if err != nil {
		return nil, fmt.Errorf("invalid key format: %v", err)
	}

	return auth.MasterKey(k)
}

func getRepositoryCredentialsFromKeyFile() (auth.Credentials, error) {
	key, err := ioutil.ReadFile(*keyFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read key file: %v", err)
	}

	return auth.MasterKey(key)
}

func getRepositoryCredentialsFromPasswordFile() (auth.Credentials, error) {
	f, err := ioutil.ReadFile(*passwordFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read password file: %v", err)
	}

	return auth.Password(strings.TrimSpace(string(f)))
}

func askForNewRepositoryPassword() (auth.Credentials, error) {
	for {
		p1, err := AskPass("Enter password to create new repository: ")
		if err != nil {
			return nil, err
		}
		p2, err := AskPass("Re-enter password for verification: ")
		if err != nil {
			return nil, err
		}
		if p1 != p2 {
			fmt.Println("Passwords don't match!")
		} else {
			return auth.Password(p1)
		}
	}
}

func askForExistingRepositoryPassword() (auth.Credentials, error) {
	p1, err := AskPass("Enter password to open repository: ")
	if err != nil {
		return nil, err
	}
	fmt.Println()
	return auth.Password(p1)
}

func getRepositoryCredentials(isNew bool) (auth.Credentials, error) {
	switch {
	case *key != "":
		return getRepositoryCredentialsFromMasterKey()
	case *password != "":
		return auth.Password(strings.TrimSpace(*password))
	case *keyFile != "":
		return getRepositoryCredentialsFromKeyFile()
	case *passwordFile != "":
		return getRepositoryCredentialsFromPasswordFile()
	case isNew:
		return askForNewRepositoryPassword()
	default:
		return askForExistingRepositoryPassword()
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

// AskPass presents a given prompt and asks the user for password.
func AskPass(prompt string) (string, error) {
	for i := 0; i < 5; i++ {
		p, err := speakeasy.Ask(prompt)
		if err != nil {
			return "", err
		}

		if len(p) == 0 {
			continue
		}

		if len(p) >= auth.MinPasswordLength {
			return p, nil
		}

		fmt.Printf("Password too short, must be at least %v characters, you entered %v. Try again.", auth.MinPasswordLength, len(p))
		fmt.Println()
	}

	return "", fmt.Errorf("can't get password")
}
