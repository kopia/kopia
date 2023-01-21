package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
)

type commandRepositoryUpgrade struct {
	forceRollback             bool
	skip                      bool
	allowUnsafeUpgradeTimings bool
	commitMode                string
	lockOnly                  bool

	// lock settings
	ioDrainTimeout         time.Duration
	statusPollInterval     time.Duration
	maxPermittedClockDrift time.Duration

	svc advancedAppServices
}

const (
	experimentalWarning = `WARNING: The upgrade command is an EXPERIMENTAL feature. Please DO NOT use it, it may corrupt your repository and cause data loss.

You will need to set the env variable KOPIA_UPGRADE_LOCK_ENABLED in order to use this feature.
`
	upgradeLockFeatureEnv         = "KOPIA_UPGRADE_LOCK_ENABLED"
	maxPermittedClockDriftDefault = 5 * time.Minute
)

const (
	commitModeAlwaysCommit = "always"
	commitModeNeverCommit  = "never"
)

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
	beginCmd.Flag("allow-unsafe-upgrade", "Force using an unsafe io-drain-timeout for the upgrade lock").Default("false").Hidden().BoolVar(&c.allowUnsafeUpgradeTimings)
	beginCmd.Flag("status-poll-interval", "An advisory polling interval to check for the status of upgrade").Default("60s").DurationVar(&c.statusPollInterval)
	beginCmd.Flag("max-permitted-clock-drift", "The maximum drift between repository and client clocks").Default(maxPermittedClockDriftDefault.String()).DurationVar(&c.maxPermittedClockDrift)
	beginCmd.Flag("lock-only", "Advertise the upgrade lock and exit without actually performing the drain or upgrade").Default("false").Hidden().BoolVar(&c.lockOnly) // this is used by tests
	beginCmd.Flag("commit-mode", "Change behavior of commit. When not set, commit on validation success. 'always': always commit. 'never': always exit before commit.").Hidden().EnumVar(&c.commitMode, commitModeAlwaysCommit, commitModeNeverCommit)

	// upgrade phases

	// Set the upgrade lock intent.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.setLockIntent)))
	// If requested then drain all the clients otherwise stop here.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.drainOrCommit)))
	// If the lock is fully established then perform the upgrade.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.upgrade)))
	// Validate index upgrade success
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.ignoreErrorOnAlwaysCommit(c.validateAction))))
	// Commit the upgrade and revoke the lock, this will also cleanup any
	// backups used for rollback.
	beginCmd.Action(svc.directRepositoryWriteAction(c.runPhase(c.commitUpgrade)))

	rollbackCmd := parent.Command("rollback", "Rollback the repository upgrade.")
	rollbackCmd.Flag("force", "Force rollback the repository upgrade, this action can cause repository corruption").BoolVar(&c.forceRollback)

	rollbackCmd.Action(svc.directRepositoryWriteAction(c.forceRollbackAction))

	validateCmd := parent.Command("validate", "Validate the upgraded indexes.")

	validateCmd.Action(svc.directRepositoryWriteAction(c.validateAction))

	c.svc = svc
}

// assign store the info struct in a map that can be used to compare indexes.
func assign(iif content.Info, i int, m map[content.ID][2]index.Info) {
	v := m[iif.GetContentID()]
	v[i] = iif
	m[iif.GetContentID()] = v
}

// loadIndexBlobs load index blobs into indexEntries map.  indexEntries map will allow comparison betweel two indexes (index at which == 0 and index at which == 1).
func loadIndexBlobs(ctx context.Context, indexEntries map[content.ID][2]index.Info, sm *content.SharedManager, which int, indexBlobInfos []indexblob.Metadata) error {
	d := gather.WriteBuffer{}

	for _, indexBlobInfo := range indexBlobInfos {
		blobID := indexBlobInfo.BlobID

		indexInfos, err := sm.LoadIndexBlob(ctx, blobID, &d)
		if err != nil {
			return errors.Wrapf(err, "failed to load index blob with BlobID %s", blobID)
		}

		for _, indexInfo := range indexInfos {
			assign(indexInfo, which, indexEntries)
		}
	}

	return nil
}

// validateAction returns an error if the new V1 index blob content does not match the source V0 index blob content.
// This is used to check that the upgraded index (V1 index) reflects the content of the old V0 index.
func (c *commandRepositoryUpgrade) validateAction(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	indexEntries := map[content.ID][2]index.Info{}

	sm := rep.ContentManager().SharedManager

	indexBlobInfos0, _, err := sm.IndexReaderV0().ListIndexBlobInfos(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to list index blobs for old index")
	}

	indexBlobInfos1, _, err := sm.IndexReaderV1().ListIndexBlobInfos(ctx)
	if err != nil {
		log(ctx).Errorf("failed to list index blobs for new index. upgrade may have failed.: %v", err)
		return nil
	}

	if len(indexBlobInfos0) == 0 && len(indexBlobInfos1) > 0 {
		log(ctx).Infof("old index is empty (possibly due to upgrade), nothing to compare against")
		return nil
	}

	// load index blobs into their appropriate positions inside the indexEntries map

	err = loadIndexBlobs(ctx, indexEntries, sm, 0, indexBlobInfos0)
	if err != nil {
		return errors.Wrapf(err, "failed to load index entries for v0 index entry")
	}

	err = loadIndexBlobs(ctx, indexEntries, sm, 1, indexBlobInfos1)
	if err != nil {
		return errors.Wrapf(err, "failed to load index entries for new index")
	}

	var msgs []string // a place to keep messages from the index comparison process

	// both indexes will have matching contentiDs with matching indexInfo structures.
	for contentID, indexEntryPairs := range indexEntries {
		iep0 := indexEntryPairs[0] // first entry of index entry pair
		iep1 := indexEntryPairs[1] // second entry of index entry pair

		// check that both the new and old indexes have entries for the same content
		if iep0 != nil && iep1 != nil {
			// this is the happy-path, check the entries.  any problems found will be added to msgs
			msgs = append(msgs, CheckIndexInfo(iep0, iep1)...)
			continue
		}

		// one of iep0 or iep1 are nil .. find out which one and add an appropriate message.
		if iep0 != nil {
			msgs = append(msgs, fmt.Sprintf("lop-sided index entries for contentID %q at blob %q", contentID, iep0.GetPackBlobID()))
			continue
		}

		msgs = append(msgs, fmt.Sprintf("lop-sided index entries for contentID %q at blob %q", contentID, iep1.GetPackBlobID()))
	}

	// no msgs means the check passed without finding anything wrong
	if len(msgs) == 0 {
		log(ctx).Infof("index validation succeeded")
		return nil
	}

	// otherwise there's a problem somewhere ... log the problems
	log(ctx).Error("inconsistencies found in migrated index:")

	for _, m := range msgs {
		log(ctx).Error(m)
	}

	// and return an error that states something's wrong.
	return errors.Wrap(err, "repository will remain locked until index differences are resolved")
}

// CheckIndexInfo compare two index infos.  If a mismatch exists, return an error with diagnostic information.
func CheckIndexInfo(i0, i1 index.Info) []string {
	var q []string

	switch {
	case i0.GetFormatVersion() != i1.GetFormatVersion():
		q = append(q, fmt.Sprintf("mismatched FormatVersions: %v %v", i0.GetFormatVersion(), i1.GetFormatVersion()))
	case i0.GetOriginalLength() != i1.GetOriginalLength():
		q = append(q, fmt.Sprintf("mismatched OriginalLengths: %v %v", i0.GetOriginalLength(), i1.GetOriginalLength()))
	case i0.GetPackBlobID() != i1.GetPackBlobID():
		q = append(q, fmt.Sprintf("mismatched PackBlobIDs: %v %v", i0.GetPackBlobID(), i1.GetPackBlobID()))
	case i0.GetPackedLength() != i1.GetPackedLength():
		q = append(q, fmt.Sprintf("mismatched PackedLengths: %v %v", i0.GetPackedLength(), i1.GetPackedLength()))
	case i0.GetPackOffset() != i1.GetPackOffset():
		q = append(q, fmt.Sprintf("mismatched PackOffsets: %v %v", i0.GetPackOffset(), i1.GetPackOffset()))
	case i0.GetEncryptionKeyID() != i1.GetEncryptionKeyID():
		q = append(q, fmt.Sprintf("mismatched EncryptionKeyIDs: %v %v", i0.GetEncryptionKeyID(), i1.GetEncryptionKeyID()))
	case i0.GetCompressionHeaderID() != i1.GetCompressionHeaderID():
		q = append(q, fmt.Sprintf("mismatched GetCompressionHeaderID: %v %v", i0.GetCompressionHeaderID(), i1.GetCompressionHeaderID()))
	case i0.GetDeleted() != i1.GetDeleted():
		q = append(q, fmt.Sprintf("mismatched Deleted flags: %v %v", i0.GetDeleted(), i1.GetDeleted()))
	case i0.GetTimestampSeconds() != i1.GetTimestampSeconds():
		q = append(q, fmt.Sprintf("mismatched TimestampSeconds: %v %v", i0.GetTimestampSeconds(), i1.GetTimestampSeconds()))
	}

	if len(q) == 0 {
		return nil
	}

	for i := range q {
		q[i] = fmt.Sprintf("index blobs do not match: %q, %q: %s", i0.GetPackBlobID(), i1.GetPackBlobID(), q[i])
	}

	return q
}

func (c *commandRepositoryUpgrade) forceRollbackAction(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if !c.forceRollback {
		return errors.New("repository upgrade lock can only be revoked unsafely; please use the --force flag")
	}

	if err := rep.FormatManager().RollbackUpgrade(ctx); err != nil {
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

func (c *commandRepositoryUpgrade) ignoreErrorOnAlwaysCommit(act func(context.Context, repo.DirectRepositoryWriter) error) func(context.Context, repo.DirectRepositoryWriter) error {
	return func(ctx context.Context, rep repo.DirectRepositoryWriter) error {
		err := act(ctx, rep)
		if err == nil {
			return nil
		}

		if c.commitMode == commitModeAlwaysCommit {
			log(ctx).Errorf("%v", err)
			return nil
		}

		// Explicitly skip all stages on error because tests do not
		// skip/exit on error. Tests override os.Exit() that prevents
		// running rest of the phases until we set the skip flag here.
		// This flag is designed for testability and also to support
		// rollback.
		c.skip = true

		return err
	}
}

// setLockIntent is an upgrade phase which sets the upgrade lock intent with
// desired parameters.
func (c *commandRepositoryUpgrade) setLockIntent(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if c.ioDrainTimeout < format.DefaultRepositoryBlobCacheDuration && !c.allowUnsafeUpgradeTimings {
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
		MaxPermittedClockDrift: c.maxPermittedClockDrift,
	}

	// Update format-blob and clear the cache.
	// This will fail if we have already upgraded.
	l, err := rep.FormatManager().SetUpgradeLockIntent(ctx, *l)
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

		l, err := rep.FormatManager().GetUpgradeLockIntent(ctx)
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
	for {
		l, err := rep.FormatManager().GetUpgradeLockIntent(ctx)

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

// upgrade phase performs the actual upgrade action that upgrades the target
// repository. This phase runs after the lock has been acquired in one of the
// prior phases.
func (c *commandRepositoryUpgrade) upgrade(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	mp, mperr := rep.ContentReader().ContentFormat().GetMutableParameters()
	if mperr != nil {
		return errors.Wrap(mperr, "mutable parameters")
	}

	rf, err := rep.FormatManager().RequiredFeatures()
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

	if uerr := rep.ContentManager().PrepareUpgradeToIndexBlobManagerV1(ctx, mp.EpochParameters); uerr != nil {
		return errors.Wrap(uerr, "error upgrading indices")
	}

	blobCfg, err := rep.FormatManager().BlobCfgBlob()
	if err != nil {
		return errors.Wrap(err, "error getting blob configuration")
	}

	// update format-blob and clear the cache
	if err := rep.FormatManager().SetParameters(ctx, mp, blobCfg, rf); err != nil {
		return errors.Wrap(err, "error setting parameters")
	}

	// we need to reopen the repository after this point

	log(ctx).Infof("Repository indices have been upgraded.")

	return nil
}

// commitUpgrade is the upgrade CLI phase that commits the upgrade and removes
// the lock after the actual upgrade phase has been run successfully. We will
// not end up here if any of the prior phases have failed. This will also
// cleanup and backups used for the rollback mechanism, so we cannot rollback
// after this phase.
func (c *commandRepositoryUpgrade) commitUpgrade(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if c.commitMode == commitModeNeverCommit {
		log(ctx).Infof("Commit mode is set to 'never'.  Skipping commit.")
		return nil
	}

	if err := rep.FormatManager().CommitUpgrade(ctx); err != nil {
		return errors.Wrap(err, "error finalizing upgrade")
	}
	// we need to reopen the repository after this point

	log(ctx).Infof("Repository has been successfully upgraded.")

	return nil
}
