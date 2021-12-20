package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandRepositorySetParameters struct {
	maxPackSizeMB           int
	indexFormatVersion      int
	disableFormatBlobCache  bool
	formatBlobCacheDuration time.Duration
	retentionMode           string
	retentionPeriod         time.Duration

	epochRefreshFrequency    time.Duration
	epochMinDuration         time.Duration
	epochCleanupSafetyMargin time.Duration
	epochAdvanceOnCount      int
	epochAdvanceOnSizeMB     int64
	epochDeleteParallelism   int
	epochCheckpointFrequency int

	svc appServices
}

func (c *commandRepositorySetParameters) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set-parameters", "Set repository parameters.").Alias("set-params")

	cmd.Flag("max-pack-size-mb", "Set max pack file size").PlaceHolder("MB").IntVar(&c.maxPackSizeMB)
	cmd.Flag("index-version", "Set version of index format used for writing").IntVar(&c.indexFormatVersion)
	cmd.Flag("retention-mode", "Set the blob retention-mode for supported storage backends.").EnumVar(&c.retentionMode, "none", blob.Governance.String(), blob.Compliance.String())
	cmd.Flag("retention-period", "Set the blob retention-period for supported storage backends.").DurationVar(&c.retentionPeriod)

	cmd.Flag("repository-format-cache-duration", "Duration of kopia.repository format blob cache").Hidden().DurationVar(&c.formatBlobCacheDuration)
	cmd.Flag("disable-repository-format-cache", "Disable caching of kopia.repository format blob").Hidden().BoolVar(&c.disableFormatBlobCache)

	cmd.Flag("epoch-refresh-frequency", "Epoch refresh frequency").DurationVar(&c.epochRefreshFrequency)
	cmd.Flag("epoch-min-duration", "Minimal duration of a single epoch").DurationVar(&c.epochMinDuration)
	cmd.Flag("epoch-cleanup-safety-margin", "Epoch cleanup safety margin").DurationVar(&c.epochCleanupSafetyMargin)
	cmd.Flag("epoch-advance-on-count", "Advance epoch if the number of indexes exceeds given threshold").IntVar(&c.epochAdvanceOnCount)
	cmd.Flag("epoch-advance-on-size-mb", "Advance epoch if the total size of indexes exceeds given threshold").Int64Var(&c.epochAdvanceOnSizeMB)
	cmd.Flag("epoch-delete-parallelism", "Epoch delete parallelism").IntVar(&c.epochDeleteParallelism)
	cmd.Flag("epoch-checkpoint-frequency", "Checkpoint frequency").IntVar(&c.epochCheckpointFrequency)

	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandRepositorySetParameters) setSizeMBParameter(ctx context.Context, v int, desc string, dst *int, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v << 20 //nolint:gomnd
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, units.BytesStringBase2(int64(v)<<20)) // nolint:gomnd
}

func (c *commandRepositorySetParameters) setInt64SizeMBParameter(ctx context.Context, v int64, desc string, dst *int64, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v << 20 //nolint:gomnd
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, units.BytesStringBase2(v<<20)) // nolint:gomnd
}

func (c *commandRepositorySetParameters) setIntParameter(ctx context.Context, v int, desc string, dst *int, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, v)
}

func (c *commandRepositorySetParameters) setDurationParameter(ctx context.Context, v time.Duration, desc string, dst *time.Duration, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, v)
}

func (c *commandRepositorySetParameters) setRetentionModeParameter(ctx context.Context, v blob.RetentionMode, desc string, dst *blob.RetentionMode, anyChange *bool) {
	if !v.IsValid() {
		return
	}

	*dst = v
	*anyChange = true

	log(ctx).Infof(" - setting %v to %s.\n", desc, v)
}

func (c *commandRepositorySetParameters) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	var anyChange bool

	mp := rep.ContentReader().ContentFormat().MutableParameters
	blobcfg := rep.BlobCfg()

	c.setSizeMBParameter(ctx, c.maxPackSizeMB, "maximum pack size", &mp.MaxPackSize, &anyChange)
	c.setIntParameter(ctx, c.indexFormatVersion, "index format version", &mp.IndexVersion, &anyChange)
	c.setDurationParameter(ctx, c.formatBlobCacheDuration, repo.FormatBlobID+" format blob cache", &mp.FormatBlobCacheDuration, &anyChange)

	if c.retentionMode == "none" {
		if blobcfg.IsRetentionEnabled() {
			log(ctx).Infof("disabling blob retention")

			blobcfg.RetentionMode = ""
			blobcfg.RetentionPeriod = 0
			anyChange = true
		}
	} else {
		c.setRetentionModeParameter(ctx, blob.RetentionMode(c.retentionMode), "storage backend blob retention mode", &blobcfg.RetentionMode, &anyChange)
		c.setDurationParameter(ctx, c.retentionPeriod, "storage backend blob retention period", &blobcfg.RetentionPeriod, &anyChange)
	}

	c.setDurationParameter(ctx, c.epochMinDuration, "minimum epoch duration", &mp.EpochParameters.MinEpochDuration, &anyChange)
	c.setDurationParameter(ctx, c.epochRefreshFrequency, "epoch refresh frequency", &mp.EpochParameters.EpochRefreshFrequency, &anyChange)
	c.setDurationParameter(ctx, c.epochCleanupSafetyMargin, "epoch cleanup safety margin", &mp.EpochParameters.CleanupSafetyMargin, &anyChange)
	c.setIntParameter(ctx, c.epochAdvanceOnCount, "epoch advance on count", &mp.EpochParameters.EpochAdvanceOnCountThreshold, &anyChange)
	c.setInt64SizeMBParameter(ctx, c.epochAdvanceOnSizeMB, "epoch advance on total size", &mp.EpochParameters.EpochAdvanceOnTotalSizeBytesThreshold, &anyChange)
	c.setIntParameter(ctx, c.epochDeleteParallelism, "epoch delete parallelism", &mp.EpochParameters.DeleteParallelism, &anyChange)
	c.setIntParameter(ctx, c.epochCheckpointFrequency, "epoch checkpoint frequency", &mp.EpochParameters.FullCheckpointFrequency, &anyChange)

	if !anyChange {
		return errors.Errorf("no changes")
	}

	if err := rep.SetParameters(ctx, mp, blobcfg); err != nil {
		return errors.Wrap(err, "error setting parameters")
	}

	log(ctx).Infof("NOTE: Repository parameters updated, you must disconnect and re-connect all other Kopia clients.")

	return nil
}
