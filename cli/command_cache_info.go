package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	cacheInfoCommand  = cacheCommands.Command("info", "Sets parameters local caching of repository data").Hidden()
	cacheInfoPathOnly = cacheInfoCommand.Flag("path", "Only display cache path").Bool()
)

func runCacheInfoCommand(ctx context.Context, rep *repo.Repository) error {
	fmt.Println(rep.CacheDirectory)
	if *cacheInfoPathOnly {
		return nil
	}

	log.Debugf("scanning cache...")
	fileCount, totalFileSize, err := scanCacheDir(filepath.Join(rep.CacheDirectory, "blocks"))
	if err != nil {
		return err
	}
	fmt.Printf("Usage: %v files %v\n", fileCount, units.BytesStringBase2(totalFileSize))
	return nil
}

func init() {
	cacheInfoCommand.Action(repositoryAction(runCacheInfoCommand))
}
