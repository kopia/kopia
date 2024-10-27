package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandRepositorySetParameters struct {
	maxPackSizeMB      int
	indexFormatVersion int
	retentionMode      string
	retentionPeriod    time.Duration

	epochRefreshFrequency    time.Duration
	epochMinDuration         time.Duration
	epochCleanupSafetyMargin time.Duration
	epochAdvanceOnCount      int
	epochAdvanceOnSizeMB     int64
	epochDeleteParallelism   int
	epochCheckpointFrequency int

	upgradeRepositoryFormat bool

	addRequiredFeature           string
	removeRequiredFeature        string
	warnOnMissingRequiredFeature bool

	svc appServices
}

func (c *commandRepositorySetParameters) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set-parameters", "Set repository parameters.").Alias("set-params")

	cmd.Flag("max-pack-size-mb", "Set max pack file size").PlaceHolder("MB").IntVar(&c.maxPackSizeMB)
	cmd.Flag("index-version", "Set version of index format used for writing").IntVar(&c.indexFormatVersion)
	cmd.Flag("retention-mode", "Set the blob retention-mode for supported storage backends.").EnumVar(&c.retentionMode, "none", blob.Governance.String(), blob.Compliance.String())
	cmd.Flag("retention-period", "Set the blob retention-period for supported storage backends.").DurationVar(&c.retentionPeriod)

	cmd.Flag("upgrade", "Upgrade repository to the latest stable format").BoolVar(&c.upgradeRepositoryFormat)

	cmd.Flag("epoch-refresh-frequency", "Epoch refresh frequency").DurationVar(&c.epochRefreshFrequency)
	cmd.Flag("epoch-min-duration", "Minimal duration of a single epoch").DurationVar(&c.epochMinDuration)
	cmd.Flag("epoch-cleanup-safety-margin", "Epoch cleanup safety margin").DurationVar(&c.epochCleanupSafetyMargin)
	cmd.Flag("epoch-advance-on-count", "Advance epoch if the number of indexes exceeds given threshold").IntVar(&c.epochAdvanceOnCount)
	cmd.Flag("epoch-advance-on-size-mb", "Advance epoch if the total size of indexes exceeds given threshold").Int64Var(&c.epochAdvanceOnSizeMB)
	cmd.Flag("epoch-delete-parallelism", "Epoch delete parallelism").IntVar(&c.epochDeleteParallelism)
	cmd.Flag("epoch-checkpoint-frequency", "Checkpoint frequency").IntVar(&c.epochCheckpointFrequency)

	if svc.enableTestOnlyFlags() {
		cmd.Flag("add-required-feature", "Add required feature which must be present to open the repository").Hidden().StringVar(&c.addRequiredFeature)
		cmd.Flag("remove-required-feature", "Remove required feature").Hidden().StringVar(&c.removeRequiredFeature)
		cmd.Flag("warn-on-missing-required-feature", "Only warn (instead of failing) when the required feature is missing").Hidden().BoolVar(&c.warnOnMissingRequiredFeature)
	}

	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func setSizeMBParameter[I ~int | ~int32 | ~int64 | ~uint | ~uint32 | ~uint64](ctx context.Context, v I, desc string, dst *I, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v << 20 //nolint:mnd
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, units.BytesString(*dst))
}

func setIntParameter(ctx context.Context, v int, desc string, dst *int, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, v)
}

func setDurationParameter(ctx context.Context, v time.Duration, desc string, dst *time.Duration, anyChange *bool) {
	if v == 0 {
		return
	}

	*dst = v
	*anyChange = true

	log(ctx).Infof(" - setting %v to %v.\n", desc, v)
}

func setRetentionModeParameter(ctx context.Context, v blob.RetentionMode, desc string, dst *blob.RetentionMode, anyChange *bool) {
	if !v.IsValid() {
		return
	}

	*dst = v
	*anyChange = true

	log(ctx).Infof(" - setting %v to %s.\n", desc, v)
}

func updateRepositoryParameters(
	ctx context.Context,
	upgradeToEpochManager bool,
	mp format.MutableParameters,
	rep repo.DirectRepositoryWriter,
	blobcfg format.BlobStorageConfiguration,
	requiredFeatures []feature.Required,
) error {
	if upgradeToEpochManager {
		log(ctx).Info("migrating current indexes to epoch format")

		if err := rep.ContentManager().PrepareUpgradeToIndexBlobManagerV1(ctx); err != nil {
			return errors.Wrap(err, "error upgrading indexes")
		}
	}

	if err := rep.FormatManager().SetParameters(ctx, mp, blobcfg, requiredFeatures); err != nil {
		return errors.Wrap(err, "error setting parameters")
	}

	if upgradeToEpochManager {
		if err := format.WriteLegacyIndexPoisonBlob(ctx, rep.BlobStorage()); err != nil {
			log(ctx).Errorf("unable to write legacy index poison blob: %v", err)
		}
	}

	return nil
}

func updateEpochParameters(mp *format.MutableParameters, anyChange, upgradeToEpochManager *bool) {
	*anyChange = true

	if !mp.EpochParameters.Enabled {
		mp.EpochParameters = epoch.DefaultParameters()
		mp.IndexVersion = 2
		*upgradeToEpochManager = true
	}

	if mp.Version < format.FormatVersion2 {
		mp.Version = format.FormatVersion2
	}
}

func disableBlobRetention(ctx context.Context, blobcfg *format.BlobStorageConfiguration, anyChange *bool) {
	log(ctx).Info("disabling blob retention")

	blobcfg.RetentionMode = ""
	blobcfg.RetentionPeriod = 0
	*anyChange = true
}

func (c *commandRepositorySetParameters) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	mp, err := rep.FormatManager().GetMutableParameters(ctx)
	if err != nil {
		return errors.Wrap(err, "mutable parameters")
	}

	blobcfg, err := rep.FormatManager().BlobCfgBlob(ctx)
	if err != nil {
		return errors.Wrap(err, "blob configuration")
	}

	requiredFeatures, err := rep.FormatManager().RequiredFeatures(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get required features")
	}

	anyChange := false
	upgradeToEpochManager := false

	if c.upgradeRepositoryFormat {
		updateEpochParameters(&mp, &anyChange, &upgradeToEpochManager)
	}

	setSizeMBParameter(ctx, c.maxPackSizeMB, "maximum pack size", &mp.MaxPackSize, &anyChange)

	// prevent downgrade of index format
	if c.indexFormatVersion != 0 && c.indexFormatVersion != mp.IndexVersion {
		if c.indexFormatVersion > mp.IndexVersion {
			setIntParameter(ctx, c.indexFormatVersion, "index format version", &mp.IndexVersion, &anyChange)
		} else {
			return errors.New("index format version can only be upgraded")
		}
	}

	if c.retentionMode == "none" {
		if blobcfg.IsRetentionEnabled() {
			// disable blob retention if already enabled
			disableBlobRetention(ctx, &blobcfg, &anyChange)
		}
	} else {
		setRetentionModeParameter(ctx, blob.RetentionMode(c.retentionMode), "storage backend blob retention mode", &blobcfg.RetentionMode, &anyChange)
		setDurationParameter(ctx, c.retentionPeriod, "storage backend blob retention period", &blobcfg.RetentionPeriod, &anyChange)
	}

	setDurationParameter(ctx, c.epochMinDuration, "minimum epoch duration", &mp.EpochParameters.MinEpochDuration, &anyChange)
	setDurationParameter(ctx, c.epochRefreshFrequency, "epoch refresh frequency", &mp.EpochParameters.EpochRefreshFrequency, &anyChange)
	setDurationParameter(ctx, c.epochCleanupSafetyMargin, "epoch cleanup safety margin", &mp.EpochParameters.CleanupSafetyMargin, &anyChange)
	setIntParameter(ctx, c.epochAdvanceOnCount, "epoch advance on count", &mp.EpochParameters.EpochAdvanceOnCountThreshold, &anyChange)
	setSizeMBParameter(ctx, c.epochAdvanceOnSizeMB, "epoch advance on total size", &mp.EpochParameters.EpochAdvanceOnTotalSizeBytesThreshold, &anyChange)
	setIntParameter(ctx, c.epochDeleteParallelism, "epoch delete parallelism", &mp.EpochParameters.DeleteParallelism, &anyChange)
	setIntParameter(ctx, c.epochCheckpointFrequency, "epoch checkpoint frequency", &mp.EpochParameters.FullCheckpointFrequency, &anyChange)

	requiredFeatures = c.addRemoveUpdateRequiredFeatures(requiredFeatures, &anyChange)

	if !anyChange {
		log(ctx).Info("no changes")
		return nil
	}

	if blobcfg.IsRetentionEnabled() {
		p, err := maintenance.GetParams(ctx, rep)
		if err != nil {
			return errors.Wrap(err, "unable to get current maintenance parameters")
		}

		if err := maintenance.CheckExtendRetention(ctx, blobcfg, p); err != nil {
			return errors.Wrap(err, "unable to apply maintenance changes")
		}
	}

	if err := updateRepositoryParameters(ctx, upgradeToEpochManager, mp, rep, blobcfg, requiredFeatures); err != nil {
		return errors.Wrap(err, "error updating repository parameters")
	}

	log(ctx).Info("NOTE: Repository parameters updated, you must disconnect and re-connect all other Kopia clients.")

	return nil
}

func (c *commandRepositorySetParameters) addRemoveUpdateRequiredFeatures(orig []feature.Required, anyChange *bool) []feature.Required {
	var result []feature.Required

	for _, v := range orig {
		if v.Feature == feature.Feature(c.removeRequiredFeature) || v.Feature == feature.Feature(c.addRequiredFeature) {
			*anyChange = true

			continue
		}

		result = append(result, v)
	}

	if c.addRequiredFeature != "" {
		result = append(result, feature.Required{
			Feature: feature.Feature(c.addRequiredFeature),
			IfNotUnderstood: feature.IfNotUnderstood{
				Warn: c.warnOnMissingRequiredFeature,
			},
		})

		*anyChange = true
	}

	return result
}
