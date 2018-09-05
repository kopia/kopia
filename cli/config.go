package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"

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

	configPath = app.Flag("config-file", "Specify the config file to use.").Default(defaultConfigFileName()).Envar("KOPIA_CONFIG_PATH").String()
)

func printStderr(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...) //nolint:errcheck
	log.Debugf("[STDERR] "+msg, args...)
}

func printStdout(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, args...) //nolint:errcheck
	log.Debugf("[STDOUT] "+msg, args...)
}

func failOnError(err error) {
	if err != nil {
		printStderr("ERROR: %v\n", err)
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
	r, err := repo.Open(ctx, repositoryConfigFileName(), mustGetPasswordFromFlags(false, true), applyOptionsFromFlags(opts))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("not connected to a repository, use 'kopia connect'")
	}

	return r, err
}

func applyOptionsFromFlags(opts *repo.Options) *repo.Options {
	if opts == nil {
		opts = &repo.Options{}
	}

	if *traceStorage {
		opts.TraceStorage = log.Debugf
	}

	if *traceObjectManager {
		opts.ObjectManagerOptions.Trace = log.Debugf
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

func mustGetLocalFSEntry(path string) fs.Entry {
	e, err := localfs.NewEntry(path)
	if err == nil {
		failOnError(err)
	}

	if *traceLocalFS {
		return loggingfs.Wrap(e, loggingfs.Prefix("[LOCALFS] "))
	}

	return e
}
