package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo"
)

var (
	traceStorage      = app.Flag("trace-storage", "Enables tracing of storage operations.").Default("true").Hidden().Bool()
	traceLocalFS      = app.Flag("trace-localfs", "Enables tracing of local filesystem operations").Envar("KOPIA_TRACE_FS").Hidden().Bool()
	metricsListenAddr = app.Flag("metrics-listen-addr", "Expose Prometheus metrics on a given host:port").Hidden().String()

	_ = app.Flag("caching", "Enables caching of objects (disable with --no-caching)").Default("true").Hidden().Action(
		deprecatedFlag("The '--caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	_ = app.Flag("list-caching", "Enables caching of list results (disable with --no-list-caching)").Default("true").Hidden().Action(
		deprecatedFlag("The '--list-caching' flag is deprecated and has no effect, use 'kopia cache set' instead."),
	).Bool()

	configPath = app.Flag("config-file", "Specify the config file to use.").Default(defaultConfigFileName()).Envar("KOPIA_CONFIG_PATH").String()
)

func deprecatedFlag(help string) func(_ *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		printStderr("DEPRECATED: %v\n", help)
		return nil
	}
}

func printStderr(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
}

func printStdout(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, args...)
}

func onCtrlC(f func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		<-c
		f()
	}()
}

func openRepository(ctx context.Context, opts *repo.Options, required bool) (repo.Repository, error) {
	if _, err := os.Stat(repositoryConfigFileName()); os.IsNotExist(err) {
		if !required {
			return nil, nil
		}

		return nil, errors.Errorf("repository is not connected. See https://kopia.io/docs/repositories/")
	}

	maybePrintUpdateNotification(ctx)

	pass, err := getPasswordFromFlags(ctx, false, true)
	if err != nil {
		return nil, errors.Wrap(err, "get password")
	}

	r, err := repo.Open(ctx, repositoryConfigFileName(), pass, optionsFromFlags(ctx))
	if os.IsNotExist(err) {
		return nil, errors.New("not connected to a repository, use 'kopia connect'")
	}

	return r, errors.Wrap(err, "unable to open repository")
}

func optionsFromFlags(ctx context.Context) *repo.Options {
	var opts repo.Options

	if *traceStorage {
		opts.TraceStorage = log(ctx).Debugf
	}

	return &opts
}

func repositoryConfigFileName() string {
	return *configPath
}

func defaultConfigFileName() string {
	return filepath.Join(ospath.ConfigDir(), "repository.config")
}

func resolveSymlink(path string) (string, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return "", errors.Wrap(err, "stat")
	}

	if (st.Mode() & os.ModeSymlink) == 0 {
		return path, nil
	}

	return filepath.EvalSymlinks(path)
}

func getLocalFSEntry(ctx context.Context, path0 string) (fs.Entry, error) {
	path, err := resolveSymlink(path0)
	if err != nil {
		return nil, errors.Wrap(err, "resolveSymlink")
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
