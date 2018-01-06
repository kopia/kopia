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
	traceStorage       = app.Flag("trace-storage", "Enables tracing of storage operations.").Hidden().Envar("KOPIA_TRACE_STORAGE").Bool()
	traceObjectManager = app.Flag("trace-object-manager", "Enables tracing of object manager operations.").Hidden().Envar("KOPIA_TRACE_OBJECT_MANAGER").Bool()
	traceLocalFS       = app.Flag("trace-localfs", "Enables tracing of local filesystem operations").Hidden().Envar("KOPIA_TRACE_FS").Bool()
	enableCaching      = app.Flag("caching", "Enables caching of objects (disable with --no-caching)").Default("true").Bool()
	enableListCaching  = app.Flag("list-caching", "Enables caching of list results (disable with --no-list-caching)").Default("true").Bool()

	configPath   = app.Flag("config-file", "Specify the config file to use.").PlaceHolder("PATH").Envar("KOPIA_CONFIG_PATH").String()
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

func getContext() context.Context {
	return context.Background()
}

func openRepository(opts *repo.Options) (*repo.Repository, error) {
	return repo.Open(getContext(), repositoryConfigFileName(), applyOptionsFromFlags(opts))
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

	opts.DisableCache = !*enableCaching
	opts.DisableListCache = !*enableListCaching

	return opts
}

func mustOpenRepository(opts *repo.Options) *repo.Repository {
	s, err := openRepository(opts)
	failOnError(err)
	return s
}

func repositoryConfigFileName() string {
	if len(*configPath) > 0 {
		return *configPath
	}

	return filepath.Join(ospath.ConfigDir(), "repository.config")
}

func getRepositoryCredentials(isNew bool) (auth.Credentials, error) {
	if *key != "" {
		k, err := hex.DecodeString(*key)
		if err != nil {
			return nil, fmt.Errorf("invalid key format: %v", err)
		}

		return auth.MasterKey(k)
	}

	if *password != "" {
		return auth.Password(strings.TrimSpace(*password))
	}

	if *keyFile != "" {
		key, err := ioutil.ReadFile(*keyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read key file: %v", err)
		}

		return auth.MasterKey(key)
	}

	if *passwordFile != "" {
		f, err := ioutil.ReadFile(*passwordFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read password file: %v", err)
		}

		return auth.Password(strings.TrimSpace(string(f)))
	}
	if isNew {
		for {
			p1, err := askPass("Enter password to create new repository: ")
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
				return auth.Password(p1)
			}
		}
	} else {
		p1, err := askPass("Enter password to open repository: ")
		if err != nil {
			return nil, err
		}
		fmt.Println()
		return auth.Password(p1)
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
	for i := 0; i < 5; i++ {
		b, err := speakeasy.Ask(prompt)
		if err != nil {
			return "", err
		}

		p := string(b)

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
