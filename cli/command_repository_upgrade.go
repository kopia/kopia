package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandRepositoryUpgrade struct {
	forceRollback bool
	skip          bool
	force         bool

	// lock settings
	advanceNoticeDuration time.Duration
	ioDrainTimeout        time.Duration
	statusPollInterval    time.Duration

	svc advancedAppServices
}

const (
	experimentalWarning = `WARNING: The upgrade command is an EXPERIMENTAL feature. Please use with caution.

You will need to set the env variable KOPIA_UPGRADE_LOCK_ENABLED in order to use this feature.
`
	upgradeLockFeatureEnv                = "KOPIA_UPGRADE_LOCK_ENABLED"
	maxPermittedClockDrift time.Duration = 5 * time.Minute
)

// MaxPermittedClockDrift is overridable interface for tests to define their
// own constants so that they do not have to wait for the default clock-drift to
// settle.
//
// nolint:gochecknoglobals
var MaxPermittedClockDrift = func() time.Duration { return maxPermittedClockDrift }

func (c *commandRepositoryUpgrade) setup(svc advancedAppServices, parent commandParent) {
	// create a sub-command - begin / rollback
	// make begin a default sub-command
	cmd := parent.Command("upgrade", fmt.Sprintf("Upgrade repository format.\n\n%s", warningColor.Sprint(experimentalWarning))).Hidden().
		Validate(func(tmpCmd *kingpin.CmdClause) error {
			if v := os.Getenv(c.svc.EnvName(upgradeLockFeatureEnv)); v == "" {
				return errors.Errorf("please set %q env variable to use this feature", upgradeLockFeatureEnv)
			}
			return nil
		})

	// TODO: cmd := parent.Command("begin", "Begin upgrade.")
	cmd.Flag("advance-notice", "Advance notice for upgrade to allow enough time for other Kopia clients to notice the lock").DurationVar(&c.advanceNoticeDuration)
	cmd.Flag("io-drain-timeout", "Max time it should take all other Kopia clients to drop repository connections").Default(repo.DefaultRepositoryBlobCacheDuration.String()).DurationVar(&c.ioDrainTimeout)
	cmd.Flag("force", "Force using an unsafe io-drain-timeout").Default("false").Hidden().BoolVar(&c.force)
	cmd.Flag("status-poll-interval", "An advisory polling interval to check for the status of upgrade").Default("60s").DurationVar(&c.statusPollInterval)
	cmd.Flag("force-rollback", "Force rollback the repository upgrade, this action can cause repository corruption").BoolVar(&c.forceRollback)

	// upgrade phases

	// Set the upgrade lock intent.
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.setLockIntent)))
	// If requested then drain all the clients otherwise stop here.
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.drainOrCommit)))
	// If the lock is fully established then perform the upgrade.
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.upgrade)))
	// Commit the upgrade and revoke the lock, this will also cleanup any
	// backups used for rollback.
	cmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.commitUpgrade)))

	c.svc = svc
}

func (c *commandRepositoryUpgrade) runPhase(act func(context.Context, repo.DirectRepositoryWriter) error) func(context.Context, repo.DirectRepositoryWriter) error {
	return func(ctx context.Context, rep repo.DirectRepositoryWriter) error {
		if !c.skip {
			if err := act(ctx, rep); err != nil {
				// Explicitly skip all stages on error because tests do not
				// skip/exit on error. Tests override os.Exit() that prevents
				// running rest of the phases until we set the skip flag here.
				// This flag is designed for testability and also to support
				// rollback.
				c.skip = true
				return err
			}
		}

		return nil
	}
}

// setLockIntent is an upgrade phase which sets the upgrade lock intent with
// desired parameters.
func (c *commandRepositoryUpgrade) setLockIntent(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if c.forceRollback {
		if err := rep.RollbackUpgrade(ctx); err != nil {
			return errors.Wrap(err, "failed to rollback the upgrade")
		}

		log(ctx).Infof("Repository upgrade lock has been revoked.")

		c.skip = true

		return nil
	}

	if c.ioDrainTimeout < repo.DefaultRepositoryBlobCacheDuration && !c.force {
		return errors.Errorf("minimum required io-drain-timeout is %s", repo.DefaultRepositoryBlobCacheDuration)
	}

	now := rep.Time()
	mp := rep.ContentReader().ContentFormat().MutableParameters
	openOpts := c.svc.optionsFromFlags(ctx)
	l := &repo.UpgradeLockIntent{
		OwnerID:                openOpts.UpgradeOwnerID,
		CreationTime:           now,
		AdvanceNoticeDuration:  c.advanceNoticeDuration,
		IODrainTimeout:         c.ioDrainTimeout,
		StatusPollInterval:     c.statusPollInterval,
		Message:                fmt.Sprintf("Upgrading from format version %d -> %d", mp.Version, content.MaxFormatVersion),
		MaxPermittedClockDrift: MaxPermittedClockDrift(),
	}

	// Update format-blob and clear the cache.
	// This will fail if we have already upgraded.
	l, err := rep.SetUpgradeLockIntent(ctx, *l)
	if err != nil {
		return errors.Wrap(err, "error setting the upgrade lock intent")
	}
	// we need to reopen the repository after this point

	locked, _ := l.IsLocked(now)
	if l.AdvanceNoticeDuration != 0 && !locked {
		upgradeTime := l.UpgradeTime()
		log(ctx).Infof("Repository upgrade advance notice has been set, you must come back and perform the upgrade at %s.",
			upgradeTime)

		c.skip = true

		return nil
	}

	log(ctx).Infof("Repository upgrade lock intent has been placed.")

	return nil
}

// drainOrCommit is the upgrade CLI phase that will actually wait for all the
// clients to be drained out of the upgrade quorum. This phase will cause all other phases to be
// skipped until the lock is fully established.
func (c *commandRepositoryUpgrade) drainOrCommit(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	cf := rep.ContentReader().ContentFormat()
	if cf.MutableParameters.EpochParameters.Enabled {
		log(ctx).Infof("Repository indices have already been migrated to the epoch format, no need to drain other clients")

		l, err := rep.GetUpgradeLockIntent(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to get upgrade lock intent")
		}

		if l.AdvanceNoticeDuration == 0 {
			// let the upgrade continue to commit the new format blob
			return nil
		}

		log(ctx).Infof("Continuing to drain since advance notice has been set")
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
		} else {
			return errors.Wrap(err, "upgrade lock got revoked after the intent was placed, giving up")
		}

		// TODO: this can get stuck
		sleepWithContext(ctx, l.StatusPollInterval)
	}

	return nil
}

// upgrade phase perfoms the actual upgrade action that upgrades the target
// repository. This phase runs after the lock has been acquired in one of the
// prior phases.
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

// commitUpgrade is the upgrade CLI phase that commits the upgrade and removes
// the lock after the actual upgrade phase has been ru nsuccessfully. We will
// not end up here if any of the prior phases have failed. This will also
// cleanup and backups used for the rollback mechanism, so we cannot rollback
// after this phase.
func (c *commandRepositoryUpgrade) commitUpgrade(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if err := rep.CommitUpgrade(ctx); err != nil {
		return errors.Wrap(err, "error finalizing upgrade")
	}
	// we need to reopen the repository after this point

	log(ctx).Infof("Repository has been successfully upgraded.")

	return nil
}
