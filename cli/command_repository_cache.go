package cli

import (
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/repo"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	cacheCommand = repositoryCommands.Command("cache", "Control local caching of repository data")

	cacheDirectory            = cacheCommand.Flag("cache-directory", "Directory where to store cache files").String()
	cacheMaxCacheSizeMB       = cacheCommand.Flag("cache-size-mb", "Size of local cache (0 disables caching)").PlaceHolder("MB").Int64()
	cacheMaxListCacheDuration = cacheCommand.Flag("max-list-cache-duration", "Duration of index cache").Default("600s").Duration()
)

func runCacheCommand(context *kingpin.ParseContext) error {
	opts := block.CachingOptions{
		CacheDirectory:          *cacheDirectory,
		MaxCacheSizeBytes:       *cacheMaxCacheSizeMB << 20,
		MaxListCacheDurationSec: int(cacheMaxListCacheDuration.Seconds()),
	}

	return repo.SetCachingConfig(getContext(), repositoryConfigFileName(), opts)
}

func init() {
	cacheCommand.Action(runCacheCommand)
}
