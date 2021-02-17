package cli

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo"
)

var (
	cacheClearCommand        = cacheCommands.Command("clear", "Clears the cache")
	cacheClearCommandPartial = cacheClearCommand.Flag("partial", "Specifies the cache to clear").Enum("contents", "indexes", "metadata", "own-writes", "blob-list")
)

func runCacheClearCommand(ctx context.Context, rep repo.DirectRepository) error {
	d := rep.CachingOptions().CacheDirectory
	if d == "" {
		return errors.New("caching not enabled")
	}

	// close repository before removing cache
	if err := rep.Close(ctx); err != nil {
		return errors.Wrap(err, "unable to close repository")
	}

	if *cacheClearCommandPartial == "" {
		return clearCacheDirectory(ctx, d)
	}

	return clearCacheDirectory(ctx, filepath.Join(d, *cacheClearCommandPartial))
}

func clearCacheDirectory(ctx context.Context, d string) error {
	log(ctx).Infof("Clearing cache directory: %v.", d)

	err := retry.WithExponentialBackoffNoValue(ctx, "delete cache", func() error {
		return os.RemoveAll(d)
	}, retry.Always)
	if err != nil {
		return errors.Wrap(err, "error removing cache directory")
	}

	if err := os.MkdirAll(d, 0o700); err != nil {
		return errors.Wrap(err, "error creating cache directory")
	}

	return nil
}

func init() {
	cacheClearCommand.Action(directRepositoryReadAction(runCacheClearCommand))
}
