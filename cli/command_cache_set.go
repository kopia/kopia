package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandCacheSetParams struct {
	directory              string
	contentCacheSizeMB     int64
	maxMetadataCacheSizeMB int64
	maxListCacheDuration   time.Duration
	contentMinSweepAge     time.Duration
	metadataMinSweepAge    time.Duration
	indexMinSweepAge       time.Duration

	svc appServices
}

func (c *commandCacheSetParams) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Sets parameters local caching of repository data")

	c.contentMinSweepAge = -1
	c.metadataMinSweepAge = -1
	c.indexMinSweepAge = -1
	c.maxListCacheDuration = -1

	cmd.Flag("cache-directory", "Directory where to store cache files").StringVar(&c.directory)
	cmd.Flag("content-cache-size-mb", "Size of local content cache").PlaceHolder("MB").Default("-1").Int64Var(&c.contentCacheSizeMB)
	cmd.Flag("content-min-sweep-age", "Minimal age of content cache item to be subject to sweeping").DurationVar(&c.contentMinSweepAge)
	cmd.Flag("metadata-cache-size-mb", "Size of local metadata cache").PlaceHolder("MB").Default("-1").Int64Var(&c.maxMetadataCacheSizeMB)
	cmd.Flag("metadata-min-sweep-age", "Minimal age of metadata cache item to be subject to sweeping").DurationVar(&c.metadataMinSweepAge)
	cmd.Flag("index-min-sweep-age", "Minimal age of index cache item to be subject to sweeping").DurationVar(&c.indexMinSweepAge)
	cmd.Flag("max-list-cache-duration", "Duration of index cache").DurationVar(&c.maxListCacheDuration)
	cmd.Action(svc.repositoryWriterAction(c.run))
	c.svc = svc
}

func (c *commandCacheSetParams) run(ctx context.Context, _ repo.RepositoryWriter) error {
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
		log(ctx).Infof("changing content cache size to %v", units.BytesString(v))
		opts.MaxCacheSizeBytes = v
		changed++
	}

	if v := c.maxMetadataCacheSizeMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing metadata cache size to %v", units.BytesString(v))
		opts.MaxMetadataCacheSizeBytes = v
		changed++
	}

	if v := c.maxListCacheDuration; v != -1 {
		log(ctx).Infof("changing list cache duration to %v", v)
		opts.MaxListCacheDuration = content.DurationSeconds(v.Seconds())
		changed++
	}

	if v := c.metadataMinSweepAge; v != -1 {
		log(ctx).Infof("changing minimum metadata sweep age to %v", v)
		opts.MinMetadataSweepAge = content.DurationSeconds(v.Seconds())
		changed++
	}

	if v := c.contentMinSweepAge; v != -1 {
		log(ctx).Infof("changing minimum content sweep age to %v", v)
		opts.MinContentSweepAge = content.DurationSeconds(v.Seconds())
		changed++
	}

	if v := c.indexMinSweepAge; v != -1 {
		log(ctx).Infof("changing minimum index sweep age to %v", v)
		opts.MinIndexSweepAge = content.DurationSeconds(v.Seconds())
		changed++
	}

	if changed == 0 {
		return errors.Errorf("no changes")
	}

	//nolint:wrapcheck
	return repo.SetCachingOptions(ctx, c.svc.repositoryConfigFileName(), opts)
}
