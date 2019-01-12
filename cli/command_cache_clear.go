package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/kopia/repo"
)

var (
	cacheClearCommand = cacheCommands.Command("clear", "Clears the cache").Hidden()
)

func runCacheClearCommand(ctx context.Context, rep *repo.Repository) error {
	if d := rep.CacheDirectory; d != "" {
		printStderr("Clearing cache directory: %v.\n", d)
		err := os.RemoveAll(d)
		if err != nil {
			return err
		}

		if err := os.MkdirAll(d, 0700); err != nil {
			return err
		}

		printStderr("Cache cleared.\n")
		return nil
	}

	return fmt.Errorf("caching not enabled")
}

func init() {
	cacheClearCommand.Action(repositoryAction(runCacheClearCommand))
}
