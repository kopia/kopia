package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

var (
	cacheSetParamsCommand = cacheCommands.Command("set", "Sets parameters local caching of repository data")

	cacheSetDirectory              = cacheSetParamsCommand.Flag("cache-directory", "Directory where to store cache files").String()
	cacheSetContentCacheSizeMB     = cacheSetParamsCommand.Flag("content-cache-size-mb", "Size of local content cache").PlaceHolder("MB").Default("-1").Int64()
	cacheSetMaxMetadataCacheSizeMB = cacheSetParamsCommand.Flag("metadata-cache-size-mb", "Size of local metadata cache").PlaceHolder("MB").Default("-1").Int64()
	cacheSetMaxListCacheDuration   = cacheSetParamsCommand.Flag("max-list-cache-duration", "Duration of index cache").Default("-1ns").Duration()
)

func runCacheSetCommand(ctx context.Context, rep *repo.DirectRepository) error {
	opts := rep.Content.CachingOptions.CloneOrDefault()

	changed := 0

	if v := *cacheSetDirectory; v != "" {
		log(ctx).Infof("setting cache directory to %v", v)
		opts.CacheDirectory = v
		changed++
	}

	if v := *cacheSetContentCacheSizeMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing content cache size to %v", units.BytesStringBase10(v))
		opts.MaxCacheSizeBytes = v
		changed++
	}

	if v := *cacheSetMaxMetadataCacheSizeMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing metadata cache size to %v", units.BytesStringBase10(v))
		opts.MaxMetadataCacheSizeBytes = v
		changed++
	}

	if v := *cacheSetMaxListCacheDuration; v != -1 {
		log(ctx).Infof("changing list cache duration to %v", v)
		opts.MaxListCacheDurationSec = int(v.Seconds())
		changed++
	}

	if changed == 0 {
		return errors.Errorf("no changes")
	}

	return rep.SetCachingConfig(ctx, opts)
}

func init() {
	cacheSetParamsCommand.Action(directRepositoryAction(runCacheSetCommand))
}
