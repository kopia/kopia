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
	"github.com/kopia/kopia/repo/format"
)

type commandRepositoryUpgrade struct {
	forceRollback bool
	skip          bool
	force         bool
	lockOnly      bool

	// lock settings
	ioDrainTimeout     time.Duration
	statusPollInterval time.Duration

	svc advancedAppServices
}

const (
	experimentalWarning = `WARNING: The upgrade command is an EXPERIMENTAL feature. Please DO NOT use it, it may corrupt your repository and cause data loss.

You will need to set the env variable KOPIA_UPGRADE_LOCK_ENABLED in order to use this feature.
`
	upgradeLockFeatureEnv                = "KOPIA_UPGRADE_LOCK_ENABLED"
	maxPermittedClockDrift time.Duration = 5 * time.Minute
)

// MaxPermittedClockDrift is overridable interface for tests to define their
// own constants so that they do not have to wait for the default clock-drift to
// settle.
//
//nolint:gochecknoglobals
var MaxPermittedClockDrift = func() time.Duration { return maxPermittedClockDrift }

func (c *commandRepositoryUpgrade) setup(svc advancedAppServices, parent commandParent) {
	// override the parent, the upgrade sub-command becomes the new parent here-onwards
	parent = parent.Command("upgrade", fmt.Sprintf("Upgrade repository format.\n\n%s", warningColor.Sprint(experimentalWarning))).Hidden().
		Validate(func(tmpCmd *kingpin.CmdClause) error {
			if v := os.Getenv(c.svc.EnvName(upgradeLockFeatureEnv)); v == "" {
				return errors.Errorf("please set %q env variable to use this feature", upgradeLockFeatureEnv)
			}
			return nil
		})

	beginCmd := parent.Command("begin", "Begin upgrade.").Default()
	beginCmd.Flag("io-drain-timeout", "Max time it should take all other Kopia clients to drop repository connections").Default(format.DefaultRepositoryBlobCacheDuration.String()).DurationVar(&c.ioDrainTimeout)
	beginCmd.Flag("allow-unsafe-upgrade", "Force using an unsafe io-drain-timeout for the upgrade lock").Default("false").Hidden().BoolVar(&c.force)
	beginCmd.Flag("status-poll-interval", "An advisory polling interval to check for the status of upgrade").Default("60s").DurationVar(&c.statusPollInterval)
	beginCmd.Flag("lock-only", "Advertise the upgrade lock and exit without actually performing the drain or upgrade").Default("false").Hidden().BoolVar(&c.lockOnly) // this is used by tests

	// upgrade phases

	// Set the upgrade lock intent.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.setLockIntent)))
	// If requested then drain all the clients otherwise stop here.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.drainOrCommit)))
	// If the lock is fully established then perform the upgrade.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.upgrade)))
	// Commit the upgrade and revoke the lock, this will also cleanup any
	// backups used for rollback.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.commitUpgrade)))

	rollbackCmd := parent.Command("rollback", "Rollback the repository upgrade.")
	rollbackCmd.Flag("force", "Force rollback the repository upgrade, this action can cause repository corruption").BoolVar(&c.forceRollback)

	rollbackCmd.Action(svc.directRepositoryWriteAction(c.forceRollbackAction))

	c.svc = svc
}

func (c *commandRepositoryUpgrade) forceRollbackAction(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if !c.forceRollback {
		return errors.New("repository upgrade lock can only be revoked unsafely; please use the --force flag")
	}

	if err := rep.RollbackUpgrade(ctx); err != nil {
		return errors.Wrap(err, "failed to rollback the upgrade")
	}

	log(ctx).Infof("Repository upgrade lock has been revoked.")

	return nil
}

func (c *commandRepositoryUpgrade) runPhase(act func(context.Context, repo.DirectRepositoryWriter) error) func(context.Context, repo.DirectRepositoryWriter) error {
	return func(ctx context.Context, rep repo.DirectRepositoryWriter) error {
		if c.skip {
			return nil
		}

		err := act(ctx, rep)
		if err != nil {
			// Explicitly skip all stages on error because tests do not
			// skip/exit on error. Tests override os.Exit() that prevents
			// running rest of the phases until we set the skip flag here.
			// This flag is designed for testability and also to support
			// rollback.
			c.skip = true
		}

		return err
	}
}

// setLockIntent is an upgrade phase which sets the upgrade lock intent with
// desired parameters.
func (c *commandRepositoryUpgrade) setLockIntent(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if c.ioDrainTimeout < format.DefaultRepositoryBlobCacheDuration && !c.force {
		return errors.Errorf("minimum required io-drain-timeout is %s", format.DefaultRepositoryBlobCacheDuration)
	}

	now := rep.Time()

	mp, mperr := rep.ContentReader().ContentFormat().GetMutableParameters()
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	openOpts := c.svc.optionsFromFlags(ctx)
	l := &format.UpgradeLockIntent{
		OwnerID:                openOpts.UpgradeOwnerID,
		CreationTime:           now,
		IODrainTimeout:         c.ioDrainTimeout,
		StatusPollInterval:     c.statusPollInterval,
		Message:                fmt.Sprintf("Upgrading from format version %d -> %d", mp.Version, format.MaxFormatVersion),
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

	// skip all other phases after this step
	if c.lockOnly {
		c.skip = true
	}

	return nil
}

// drainOrCommit is the upgrade CLI phase that will actually wait for all the
// clients to be drained out of the upgrade quorum. This phase will cause all other phases to be
// skipped until the lock is fully established.
func (c *commandRepositoryUpgrade) drainOrCommit(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	cf := rep.ContentReader().ContentFormat()

	mp, mperr := cf.GetMutableParameters()
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	if mp.EpochParameters.Enabled {
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

func (c *commandRepositoryUpgrade) sleepWithContext(ctx context.Context, dur time.Duration) bool {
	t := time.NewTimer(dur)
	defer t.Stop()

	stop := make(chan struct{})

	c.svc.onCtrlC(func() { close(stop) })

	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	case <-t.C:
		return true
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
		l, err := format.ReadAndCacheRepoUpgradeLock(ctx, rep.BlobStorage(), password, cacheOpts.CacheDirectory, -1)
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
		if !c.sleepWithContext(ctx, l.StatusPollInterval) {
			return errors.Errorf("upgrade drain interrupted")
		}
	}

	return nil
}

// upgrade phase perfoms the actual upgrade action that upgrades the target
// repository. This phase runs after the lock has been acquired in one of the
// prior phases.
func (c *commandRepositoryUpgrade) upgrade(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	mp, mperr := rep.ContentReader().ContentFormat().GetMutableParameters()
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	rf, err := rep.RequiredFeatures()
	if err != nil {
		return errors.Wrap(err, "error getting repository features")
	}

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
	if err := rep.SetParameters(ctx, mp, rep.BlobCfg(), rf); err != nil {
		return errors.Wrap(err, "error setting parameters")
	}

	// poison V0 index so that old readers won't be able to open it.
	if err := content.WriteLegacyIndexPoisonBlob(ctx, rep.BlobStorage()); err != nil {
		log(ctx).Errorf("unable to write legacy index poison blob: %v", err)
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
