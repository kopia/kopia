package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"

	"github.com/pkg/errors"

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
}

func printStdout(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, args...) //nolint:errcheck
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

func openRepository(ctx context.Context, opts *repo.Options, required bool) (*repo.Repository, error) {
	if _, err := os.Stat(repositoryConfigFileName()); os.IsNotExist(err) && !required {
		return nil, nil
	}

	pass, err := getPasswordFromFlags(ctx, false, true)
	if err != nil {
		return nil, errors.Wrap(err, "get password")
	}

	r, err := repo.Open(ctx, repositoryConfigFileName(), pass, applyOptionsFromFlags(ctx, opts))
	if os.IsNotExist(err) {
		return nil, errors.New("not connected to a repository, use 'kopia connect'")
	}

	return r, err
}

func applyOptionsFromFlags(ctx context.Context, opts *repo.Options) *repo.Options {
	if opts == nil {
		opts = &repo.Options{}
	}

	if *traceStorage {
		opts.TraceStorage = log(ctx).Debugf
	}

	if *traceObjectManager {
		opts.ObjectManagerOptions.Trace = log(ctx).Debugf
	}

	return opts
}

func repositoryConfigFileName() string {
	return *configPath
}

func defaultConfigFileName() string {
	return filepath.Join(ospath.ConfigDir(), "repository.config")
}

func getLocalFSEntry(ctx context.Context, path0 string) (fs.Entry, error) {
	path, err := filepath.EvalSymlinks(path0)
	if err != nil {
		return nil, errors.Wrap(err, "evaluate symlink")
	}

	if path != path0 {
		log(ctx).Infof("%v resolved to %v", path0, path)
	}

	e, err := localfs.NewEntry(path)
	if err != nil {
		return nil, errors.Wrap(err, "can't get local fs entry")
	}

	if *traceLocalFS {
		e = loggingfs.Wrap(e, log(ctx).Debugf, loggingfs.Prefix("[LOCALFS] "))
	}

	return e, nil
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}
