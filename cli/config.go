package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/ospath"
	"github.com/kopia/kopia/repo"
)

// Report usage of deprecated flag, for use in options initializer.
func (c *App) deprecatedFlagUsed(flagDeprecated, envDeprecated, flag, env string) error {
	fmt.Fprintf(c.stderrWriter, "DEPRECATED: The '%v' flag ($%v) is deprecated, use '%v' ($%v) instead.\n", flagDeprecated, envDeprecated, flag, env) //nolint:errcheck

	if c.strictArgs {
		return errors.New("deprecated argument used when in 'strict args' mode")
	}

	return nil
}

// Report conflict of deprecated flag, for use in options initializer.
func (c *App) deprecatedFlagConflict(flagDeprecated, envDeprecated, flag, env string) error {
	fmt.Fprintf(c.stderrWriter, "DEPRECATED: The '%v' flag ($%v) is deprecated, and '%v' ($%v) was also provided. Remove the deprecated flag (or environment variable).\n", flagDeprecated, envDeprecated, flag, env) //nolint:errcheck
	return errors.New("deprecated argument conflict")
}

// Merge and detect conflicts of deprecated flags, for use in options initializer.
func (c *App) mergeDeprecatedFlags(valueDeprecated, value, flagDeprecated, envDeprecated, flag, env string) (string, error) {
	if valueDeprecated == "" {
		// no deprecated value to merge
		return value, nil
	}

	if value == "" {
		// use deprecated value for compatibility
		err := c.deprecatedFlagUsed(flagDeprecated, envDeprecated, flag, env)
		return valueDeprecated, err
	}

	// both new and deprecated values specified, report conflict
	err := c.deprecatedFlagConflict(flagDeprecated, envDeprecated, flag, env)

	return "", err
}

func (c *App) onRepositoryFatalError(f func(err error)) {
	c.onFatalErrorCallbacks = append(c.onFatalErrorCallbacks, f)
}

func (c *App) onTerminate(f func()) {
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Interrupt, syscall.SIGTERM)

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

		return nil, errors.New("repository is not connected. See https://kopia.io/docs/repositories/")
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
		TraceStorage:         c.traceStorage,
		DisableRepositoryLog: c.disableRepositoryLog,
		UpgradeOwnerID:       c.upgradeOwnerID,
		DoNotWaitForUpgrade:  c.doNotWaitForUpgrade,
		ContentLogWriter:     c.contentLogWriter,

		// when a fatal error is encountered in the repository, run all registered callbacks
		// and exit the program.
		OnFatalError: func(err error) {
			log(ctx).Debugf("onFatalError: %v", err)

			for _, cb := range c.onFatalErrorCallbacks {
				cb(err)
			}

			c.exitWithError(err)
		},

		TestOnlyIgnoreMissingRequiredFeatures: c.testonlyIgnoreMissingRequiredFeatures,
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

	//nolint:wrapcheck
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
