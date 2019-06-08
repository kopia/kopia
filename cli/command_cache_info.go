package cli

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	cacheInfoCommand  = cacheCommands.Command("info", "Displays cache information and statistics")
	cacheInfoPathOnly = cacheInfoCommand.Flag("path", "Only display cache path").Bool()
)

func runCacheInfoCommand(ctx context.Context, rep *repo.Repository) error {
	fmt.Println(rep.CacheDirectory)
	if *cacheInfoPathOnly {
		return nil
	}

	log.Debugf("scanning contents cache...")
	fileCount, totalFileSize, err := scanCacheDir(filepath.Join(rep.CacheDirectory, "contents"))
	if err != nil {
		return err
	}
	fmt.Printf("Content usage: %v files %v\n", fileCount, units.BytesStringBase2(totalFileSize))

	log.Debugf("scanning metadata cache...")
	metadataFileCount, totalMetadataFileSize, err := scanCacheDir(filepath.Join(rep.CacheDirectory, "metadata"))
	if err != nil {
		return err
	}
	fmt.Printf("Metadata usage: %v files %v\n", metadataFileCount, units.BytesStringBase2(totalMetadataFileSize))
	return nil
}

func init() {
	cacheInfoCommand.Action(repositoryAction(runCacheInfoCommand))
}
