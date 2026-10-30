package cli

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type cacheSizeFlags struct {
	contentCacheSizeMB      int64
	contentCacheSizeLimitMB int64
	contentMinSweepAge      time.Duration

	metadataCacheSizeMB      int64
	metadataCacheSizeLimitMB int64
	metadataMinSweepAge      time.Duration

	maxListCacheDuration time.Duration
	indexMinSweepAge     time.Duration
}

func (c *cacheSizeFlags) setup(cmd *kingpin.CmdClause) {
	// do not use Defaults here, since this structure is shared between connect/create/set commands
	// each command will set their default values in code.
	cmd.Flag("content-cache-size-mb", "Desired size of local content cache (soft limit)").PlaceHolder("MB").Int64Var(&c.contentCacheSizeMB)
	cmd.Flag("content-cache-size-limit-mb", "Maximum size of local content cache (hard limit)").PlaceHolder("MB").Int64Var(&c.contentCacheSizeLimitMB)
	cmd.Flag("content-min-sweep-age", "Minimal age of content cache item to be subject to sweeping").DurationVar(&c.contentMinSweepAge)
	cmd.Flag("metadata-cache-size-mb", "Desired size of local metadata cache (soft limit)").PlaceHolder("MB").Int64Var(&c.metadataCacheSizeMB)
	cmd.Flag("metadata-cache-size-limit-mb", "Maximum size of local metadata cache (hard limit)").PlaceHolder("MB").Int64Var(&c.metadataCacheSizeLimitMB)
	cmd.Flag("metadata-min-sweep-age", "Minimal age of metadata cache item to be subject to sweeping").DurationVar(&c.metadataMinSweepAge)
	cmd.Flag("index-min-sweep-age", "Minimal age of index cache item to be subject to sweeping").DurationVar(&c.indexMinSweepAge)
	cmd.Flag("max-list-cache-duration", "Duration of index cache").DurationVar(&c.maxListCacheDuration)
}

type commandCacheSetParams struct {
	directory string

	cacheSizeFlags

	svc appServices
}

func (c *commandCacheSetParams) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Sets parameters local caching of repository data")

	c.contentMinSweepAge = -1
	c.metadataMinSweepAge = -1
	c.indexMinSweepAge = -1
	c.maxListCacheDuration = -1
	c.contentCacheSizeLimitMB = -1
	c.contentCacheSizeMB = -1
	c.metadataCacheSizeLimitMB = -1
	c.metadataCacheSizeMB = -1
	c.cacheSizeFlags.setup(cmd)

	cmd.Flag("cache-directory", "Directory where to store cache files").StringVar(&c.directory)

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
		opts.ContentCacheSizeBytes = v
		changed++
	}

	if v := c.contentCacheSizeLimitMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing content cache size limit to %v", units.BytesString(v))
		opts.ContentCacheSizeLimitBytes = v
		changed++
	}

	if v := c.metadataCacheSizeMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing metadata cache size to %v", units.BytesString(v))
		opts.MetadataCacheSizeBytes = v
		changed++
	}

	if v := c.metadataCacheSizeLimitMB; v != -1 {
		v *= 1e6 // convert MB to bytes
		log(ctx).Infof("changing metadata cache size limit to %v", units.BytesString(v))
		opts.MetadataCacheSizeLimitBytes = v
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
		return errors.New("no changes")
	}

	//nolint:wrapcheck
	return repo.SetCachingOptions(ctx, c.svc.repositoryConfigFileName(), opts)
}
