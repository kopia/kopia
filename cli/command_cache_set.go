package cli

import (
	"context"

	"github.com/kopia/repo"
	"github.com/kopia/repo/block"
)

var (
	cacheSetParamsCommand = cacheCommands.Command("set", "Sets parameters local caching of repository data")

	cacheSetDirectory            = cacheSetParamsCommand.Flag("cache-directory", "Directory where to store cache files").String()
	cacheSetMaxCacheSizeMB       = cacheSetParamsCommand.Flag("cache-size-mb", "Size of local cache (0 disables caching)").PlaceHolder("MB").Int64()
	cacheSetMaxListCacheDuration = cacheSetParamsCommand.Flag("max-list-cache-duration", "Duration of index cache").Default("600s").Duration()
)

func runCacheSetCommand(ctx context.Context, rep *repo.Repository) error {
	opts := block.CachingOptions{
		CacheDirectory:          *cacheSetDirectory,
		MaxCacheSizeBytes:       *cacheSetMaxCacheSizeMB << 20,
		MaxListCacheDurationSec: int(cacheSetMaxListCacheDuration.Seconds()),
	}

	return repo.SetCachingConfig(ctx, repositoryConfigFileName(), opts)
}

func init() {
	cacheSetParamsCommand.Action(repositoryAction(runCacheSetCommand))
}
