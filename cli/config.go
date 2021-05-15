package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo"
)

func deprecatedFlag(w io.Writer, help string) func(_ *kingpin.ParseContext) error {
	return func(_ *kingpin.ParseContext) error {
		fmt.Fprintf(w, "DEPRECATED: %v\n", help)
		return nil
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

func (c *App) openRepository(ctx context.Context, required bool) (repo.Repository, error) {
	if _, err := os.Stat(c.repositoryConfigFileName()); os.IsNotExist(err) {
		if !required {
			return nil, nil
		}

		return nil, errors.Errorf("repository is not connected. See https://kopia.io/docs/repositories/")
	}

	c.maybePrintUpdateNotification(ctx)

	pass, err := c.getPasswordFromFlags(ctx, false, true)
	if err != nil {
		return nil, errors.Wrap(err, "get password")
	}

	r, err := repo.Open(ctx, c.repositoryConfigFileName(), pass, c.optionsFromFlags(ctx))
	if os.IsNotExist(err) {
		return nil, errors.New("not connected to a repository, use 'kopia connect'")
	}

	return r, errors.Wrap(err, "unable to open repository")
}

func (c *App) optionsFromFlags(ctx context.Context) *repo.Options {
	var opts repo.Options

	if c.traceStorage {
		opts.TraceStorage = log(ctx).Debugf
	}

	return &opts
}

func (c *App) repositoryConfigFileName() string {
	return c.configPath
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

	// nolint:wrapcheck
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

	return e, nil
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}
