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

func (c *App) onCtrlC(f func()) {
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt)

	go func() {
		// invoke the function when either real or simulated Ctrl-C signal is delivered
		select {
		case v := <-c.simulatedCtrlC:
			if !v {
				return
			}

		case <-s:
		}
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
	return &repo.Options{
		TraceStorage:        c.traceStorage,
		DisableInternalLog:  c.disableInternalLog,
		UpgradeOwnerID:      c.upgradeOwnerID,
		DoNotWaitForUpgrade: c.doNotWaitForUpgrade,
	}
}

func (c *App) repositoryConfigFileName() string {
	if filepath.Base(c.configPath) != c.configPath {
		return c.configPath
	}

	// bare filename specified without any directory (absolute or relative)
	// resolve against OS-specific directory.
	return filepath.Join(ospath.ConfigDir(), c.configPath)
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
