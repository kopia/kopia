package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	cacheInfoCommand  = cacheCommands.Command("info", "Displays cache information and statistics").Default()
	cacheInfoPathOnly = cacheInfoCommand.Flag("path", "Only display cache path").Bool()
)

func runCacheInfoCommand(ctx context.Context, rep *repo.Repository) error {
	if *cacheInfoPathOnly {
		fmt.Println(rep.CacheDirectory)
		return nil
	}

	entries, err := ioutil.ReadDir(rep.CacheDirectory)
	if err != nil {
		return errors.Wrap(err, "unable to scan cache directory")
	}

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		subdir := filepath.Join(rep.CacheDirectory, ent.Name())
		fileCount, totalFileSize, err := scanCacheDir(subdir)
		if err != nil {
			return err
		}
		fmt.Printf("%v: %v files %v\n", subdir, fileCount, units.BytesStringBase2(totalFileSize))
	}

	return nil
}

func init() {
	cacheInfoCommand.Action(repositoryAction(runCacheInfoCommand))
}
