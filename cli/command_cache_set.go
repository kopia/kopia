package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	cacheSetParamsCommand = cacheCommands.Command("set", "Sets parameters local caching of repository data")

	cacheSetDirectory              = cacheSetParamsCommand.Flag("cache-directory", "Directory where to store cache files").String()
	cacheSetMaxCacheSizeMB         = cacheSetParamsCommand.Flag("cache-size-mb", "Size of local content cache (0 disables caching)").PlaceHolder("MB").Int64()
	cacheSetMaxMetadataCacheSizeMB = cacheSetParamsCommand.Flag("metadata-cache-size-mb", "Size of local metadata cache (0 disables caching)").PlaceHolder("MB").Int64()
	cacheSetMaxListCacheDuration   = cacheSetParamsCommand.Flag("max-list-cache-duration", "Duration of index cache").Default("600s").Duration()
)

func runCacheSetCommand(ctx context.Context, rep *repo.Repository) error {
	opts := content.CachingOptions{
		CacheDirectory:            *cacheSetDirectory,
		MaxCacheSizeBytes:         *cacheSetMaxCacheSizeMB << 20,
		MaxMetadataCacheSizeBytes: *cacheSetMaxMetadataCacheSizeMB << 20,
		MaxListCacheDurationSec:   int(cacheSetMaxListCacheDuration.Seconds()),
	}

	return repo.SetCachingConfig(ctx, repositoryConfigFileName(), opts)
}

func init() {
	cacheSetParamsCommand.Action(repositoryAction(runCacheSetCommand))
}
