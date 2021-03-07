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

func runCacheInfoCommand(ctx context.Context, rep repo.Repository) error {
	opts, err := repo.GetCachingOptions(ctx, repositoryConfigFileName())
	if err != nil {
		return errors.Wrap(err, "error getting cache options")
	}

	if *cacheInfoPathOnly {
		fmt.Println(opts.CacheDirectory)
		return nil
	}

	entries, err := ioutil.ReadDir(opts.CacheDirectory)
	if err != nil {
		return errors.Wrap(err, "unable to scan cache directory")
	}

	path2Limit := map[string]int64{
		"contents":        opts.MaxCacheSizeBytes,
		"metadata":        opts.MaxMetadataCacheSizeBytes,
		"server-contents": opts.MaxCacheSizeBytes,
	}

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}

		subdir := filepath.Join(opts.CacheDirectory, ent.Name())

		fileCount, totalFileSize, err := scanCacheDir(subdir)
		if err != nil {
			return err
		}

		maybeLimit := ""
		if l, ok := path2Limit[ent.Name()]; ok {
			maybeLimit = fmt.Sprintf(" (limit %v)", units.BytesStringBase10(l))
		}

		if ent.Name() == "blob-list" {
			maybeLimit = fmt.Sprintf(" (duration %vs)", opts.MaxListCacheDurationSec)
		}

		fmt.Printf("%v: %v files %v%v\n", subdir, fileCount, units.BytesStringBase10(totalFileSize), maybeLimit)
	}

	printStderr("To adjust cache sizes use 'kopia cache set'.\n")
	printStderr("To clear caches use 'kopia cache clear'.\n")

	return nil
}

func init() {
	cacheInfoCommand.Action(repositoryReaderAction(runCacheInfoCommand))
}
