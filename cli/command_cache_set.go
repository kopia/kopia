package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

type commandCacheSetParams struct {
	directory              string
	contentCacheSizeMB     int64
	maxMetadataCacheSizeMB int64
	maxListCacheDuration   time.Duration

	svc appServices
}

func (c *commandCacheSetParams) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Sets parameters local caching of repository data")

	cmd.Flag("cache-directory", "Directory where to store cache files").StringVar(&c.directory)
	cmd.Flag("content-cache-size-mb", "Size of local content cache").PlaceHolder("MB").Default("-1").Int64Var(&c.contentCacheSizeMB)
	cmd.Flag("metadata-cache-size-mb", "Size of local metadata cache").PlaceHolder("MB").Default("-1").Int64Var(&c.maxMetadataCacheSizeMB)
	cmd.Flag("max-list-cache-duration", "Duration of index cache").Default("-1ns").DurationVar(&c.maxListCacheDuration)
	cmd.Action(svc.repositoryWriterAction(c.run))
	c.svc = svc
}

func (c *commandCacheSetParams) run(ctx context.Context, rep repo.RepositoryWriter) error {
	opts, err := repo.GetCachingOptions(ctx, c.svc.repositoryConfigFileName())
	if err != nil {
		return errors.Wrap(err, "error getting caching options")
	}

	changed := 0

	if v := c.directory; v != "" {
		log(ctx).Infof("setting cache directory to %v", v)
		opts.CacheDirectory = v
		changed++
	}

	if v := c.contentCacheSizeMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing content cache size to %v", units.BytesStringBase10(v))
		opts.MaxCacheSizeBytes = v
		changed++
	}

	if v := c.maxMetadataCacheSizeMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing metadata cache size to %v", units.BytesStringBase10(v))
		opts.MaxMetadataCacheSizeBytes = v
		changed++
	}

	if v := c.maxListCacheDuration; v != -1 {
		log(ctx).Infof("changing list cache duration to %v", v)
		opts.MaxListCacheDurationSec = int(v.Seconds())
		changed++
	}

	if changed == 0 {
		return errors.Errorf("no changes")
	}

	// nolint:wrapcheck
	return repo.SetCachingOptions(ctx, c.svc.repositoryConfigFileName(), opts)
}
