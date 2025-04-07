package cli

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo"
)

type commandCacheClear struct {
	partial string

	svc appServices
}

func (c *commandCacheClear) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("clear", "Clears the cache")
	cmd.Flag("partial", "Specifies the cache to clear").EnumVar(&c.partial, "contents", "indexes", "metadata", "own-writes", "blob-list")
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.svc = svc
}

func (c *commandCacheClear) run(ctx context.Context, rep repo.Repository) error {
	opts, err := repo.GetCachingOptions(ctx, c.svc.repositoryConfigFileName())
	if err != nil {
		return errors.Wrap(err, "error getting caching options")
	}

	d := opts.CacheDirectory
	if d == "" {
		return errors.New("caching not enabled")
	}

	// close repository before removing cache
	if err := rep.Close(ctx); err != nil {
		return errors.Wrap(err, "unable to close repository")
	}

	if c.partial == "" {
		return clearCacheDirectory(ctx, d)
	}

	return clearCacheDirectory(ctx, filepath.Join(d, c.partial))
}

func clearCacheDirectory(ctx context.Context, d string) error {
	log(ctx).Infof("Clearing cache directory: %v.", d)

	err := retry.WithExponentialBackoffNoValue(ctx, "delete cache", func() error {
		return os.RemoveAll(d)
	}, retry.Always)
	if err != nil {
		return errors.Wrap(err, "error removing cache directory")
	}

	if err := os.MkdirAll(d, cache.DirMode); err != nil {
		return errors.Wrap(err, "error creating cache directory")
	}

	return nil
}
