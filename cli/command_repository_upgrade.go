package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandRepositoryUpgrade struct {
	forceRollback   bool
	skip            bool
	blockUntilDrain bool

	// lock settings
	advanceNoticeInterval  time.Duration
	ioDrainTimeout         time.Duration
	statusPollInterval     time.Duration
	maxPermittedClockDrift time.Duration

	svc advancedAppServices
}

func (c *commandRepositoryUpgrade) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("upgrade", "Upgrade repository format.")

	cmd.Flag("advance-notice", "Advance notice for upgrade to allow enough time for other Kopia clients to notice the lock").DurationVar(&c.advanceNoticeInterval)
	cmd.Flag("io-drain-timeout", "Max time it should take all other Kopia clients to drop repository connections").Default(repo.DefaultRepositoryBlobCacheDuration.String()).DurationVar(&c.ioDrainTimeout)
	cmd.Flag("status-poll-interval", "An advisory polling interval to check for the status of upgrade").Default("60s").DurationVar(&c.statusPollInterval)
	cmd.Flag("max-clock-drift", "Maximum tolerated drift on clocks between all Kopia clients").Default("5m").DurationVar(&c.maxPermittedClockDrift)
	cmd.Flag("block-until-drain", "Drain all the clients but do not perform the upgrade").BoolVar(&c.blockUntilDrain)

	cmd.Flag("force-rollback", "Force rollback the repository upgrade, this action can cause repository corruption").BoolVar(&c.forceRollback)

	// upgrade sequence
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.setLockIntent)))
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.drainOrCommit)))
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.upgrade)))
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.commitUpgrade)))

	c.svc = svc
}

func (c *commandRepositoryUpgrade) runPhase(act func(context.Context, repo.DirectRepositoryWriter) error) func(context.Context, repo.DirectRepositoryWriter) error {
	return func(ctx context.Context, rep repo.DirectRepositoryWriter) error {
		if !c.skip {
			if err := act(ctx, rep); err != nil {
				// explicitly skip all stages on error because tests do not
				// skip/exit on error because they override os.Exit()
				c.skip = true
				return err
			}
		}

		return nil
	}
}

func (c *commandRepositoryUpgrade) setLockIntent(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if c.forceRollback {
		if err := rep.RollbackUpgrade(ctx); err != nil {
			return errors.Wrap(err, "failed to rollback the upgrade")
		}

		log(ctx).Infof("Repository upgrade lock has been revoked.")

		c.skip = true

		return nil
	}

	now := rep.Time()
	mp := rep.ContentReader().ContentFormat().MutableParameters
	openOpts := c.svc.optionsFromFlags(ctx)
	l := &content.UpgradeLock{
		OwnerID:                openOpts.UpgradeOwnerID,
		CreationTime:           now,
		AdvanceNoticeDuration:  c.advanceNoticeInterval,
		IODrainTimeout:         c.ioDrainTimeout,
		StatusPollInterval:     c.statusPollInterval,
		Message:                fmt.Sprintf("Upgrading from format version %d -> %d", mp.Version, content.MaxFormatVersion),
		MaxPermittedClockDrift: c.maxPermittedClockDrift,
	}

	// Update format-blob and clear the cache.
	// This will fail if we have alread upgraded.
	l, err := rep.SetUpgradeLockIntent(ctx, *l)
	if err != nil {
		return errors.Wrap(err, "error setting the upgrade lock intent")
	}
	// we need to reopen the repository after this point

	locked, _ := l.IsLocked(now)
	if l.AdvanceNoticeDuration != 0 && !locked && !c.blockUntilDrain {
		upgradeTime := l.UpgradeTime()
		log(ctx).Infof("Repository upgrade advance notice has been set, you must come back and perform the upgrade at %s.",
			upgradeTime)

		c.skip = true

		return nil
	}

	log(ctx).Infof("Repository upgrade lock intent has been placed.")

	return nil
}

func (c *commandRepositoryUpgrade) drainOrCommit(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	// skip next phases if requested
	if c.blockUntilDrain {
		c.skip = true
	}

	cf := rep.ContentReader().ContentFormat()
	if cf.MutableParameters.EpochParameters.Enabled {
		log(ctx).Infof("Repository indices have already been migrated to the epoch format, no need to drain other clients")

		if cf.UpgradeLock.AdvanceNoticeDuration == 0 || !c.blockUntilDrain {
			// let the upgrade continue to commit the new format blob
			return nil
		}

		log(ctx).Infof("Continuing to drain since advance notice has been set and we have been requested to block until then")
	}

	if err := c.drainAllClients(ctx, rep); err != nil {
		return errors.Wrap(err, "failed to upgrade the repository, lock is not released")
	}
	// we need to reopen the repository after this point

	log(ctx).Infof("Successfully drained all repository clients, the lock has been fully-established now.")

	return nil
}

// TODO(small): Better reuse.
func sleepWithContext(ctx context.Context, dur time.Duration) {
	t := time.NewTimer(dur)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (c *commandRepositoryUpgrade) drainAllClients(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	password, err := c.svc.getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	configFile, err := filepath.Abs(c.svc.repositoryConfigFileName())
	if err != nil {
		return errors.Wrap(err, "error resolving config file path")
	}

	lc, err := repo.LoadConfigFromFile(configFile)
	if err != nil {
		return errors.Wrapf(err, "error loading config file %q", configFile)
	}

	cacheOpts := lc.Caching.CloneOrDefault()

	for {
		l, err := repo.ReadAndCacheRepoUpgradeLock(ctx, rep.BlobStorage(), password, cacheOpts, -1)
		if err != nil {
			return errors.Wrap(err, "unable to reload the repository format blob")
		}

		upgradeTime := l.UpgradeTime()
		now := rep.Time()

		log(ctx).Infof("Waiting for %s to allow all other kopia clients to drain ...", upgradeTime.Sub(rep.Time()).Round(time.Second))

		locked, writersDrained := l.IsLocked(now)
		if locked {
			if writersDrained {
				// we have the lock now
				break
			}
		} else if !c.blockUntilDrain {
			return errors.Wrap(err, "upgrade lock got revoked after the intent was placed, giving up")
		}

		// TODO: this can get stuck
		sleepWithContext(ctx, l.StatusPollInterval)
	}

	return nil
}

func (c *commandRepositoryUpgrade) upgrade(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	mp := rep.ContentReader().ContentFormat().MutableParameters
	if mp.EpochParameters.Enabled {
		// nothing to upgrade on format, so let the next action commit the upgraded format blob
		return nil
	}

	mp.EpochParameters = epoch.DefaultParameters()
	mp.IndexVersion = 2

	log(ctx).Infof("migrating current indices to epoch format")

	if err := rep.ContentManager().PrepareUpgradeToIndexBlobManagerV1(ctx, mp.EpochParameters); err != nil {
		return errors.Wrap(err, "error upgrading indices")
	}

	// update format-blob and clear the cache
	if err := rep.SetParameters(ctx, mp, rep.BlobCfg()); err != nil {
		return errors.Wrap(err, "error setting parameters")
	}
	// we need to reopen the repository after this point

	log(ctx).Infof("Repository indices have been upgraded.")

	return nil
}

func (c *commandRepositoryUpgrade) commitUpgrade(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if err := rep.CommitUpgrade(ctx); err != nil {
		return errors.Wrap(err, "error finalizing upgrade")
	}
	// we need to reopen the repository after this point

	log(ctx).Infof("Repository has been successfully upgraded.")

	return nil
}
