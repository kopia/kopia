package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
)

var (
	cacheCommand = repositoryCommands.Command("setcacheparams", "Control local caching of repository data").Hidden()

	cacheDirectory            = cacheCommand.Flag("cache-directory", "Directory where to store cache files").String()
	cacheMaxCacheSizeMB       = cacheCommand.Flag("cache-size-mb", "Size of local cache (0 disables caching)").PlaceHolder("MB").Int64()
	cacheMaxListCacheDuration = cacheCommand.Flag("max-list-cache-duration", "Duration of index cache").Default("600s").Duration()
)

func runCacheCommand(ctx context.Context, rep *repo.Repository) error {
	opts := block.CachingOptions{
		CacheDirectory:          *cacheDirectory,
		MaxCacheSizeBytes:       *cacheMaxCacheSizeMB << 20,
		MaxListCacheDurationSec: int(cacheMaxListCacheDuration.Seconds()),
	}

	return repo.SetCachingConfig(ctx, repositoryConfigFileName(), opts)
}

func init() {
	cacheCommand.Action(repositoryAction(runCacheCommand))
}
