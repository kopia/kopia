package cli

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo"
)

var (
	cacheClearCommand = cacheCommands.Command("clear", "Clears the cache")
)

func runCacheClearCommand(ctx context.Context, rep *repo.DirectRepository) error {
	if d := rep.Content.CachingOptions.CacheDirectory; d != "" {
		printStderr("Clearing cache directory: %v.\n", d)

		// close repository before removing cache
		if err := rep.Close(ctx); err != nil {
			return errors.Wrap(err, "unable to close repository")
		}

		err := retry.WithExponentialBackoffNoValue(ctx, "delete cache", func() error {
			return os.RemoveAll(d)
		}, retry.Always)
		if err != nil {
			return err
		}

		if err := os.MkdirAll(d, 0700); err != nil {
			return err
		}

		printStderr("Cache cleared.\n")

		return nil
	}

	return errors.New("caching not enabled")
}

func init() {
	cacheClearCommand.Action(directRepositoryAction(runCacheClearCommand))
}
