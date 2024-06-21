package cli

import (
	"context"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandSnapshotMigrate struct {
	migrateSourceConfig      string
	migrateSources           []string
	migrateAll               bool
	migratePolicies          bool
	migrateOverwritePolicies bool
	migrateLatestOnly        bool
	migrateParallel          int
	applyIgnoreRules         bool

	svc advancedAppServices
	out textOutput
}

func (c *commandSnapshotMigrate) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("migrate", "Migrate snapshots from another repository")
	cmd.Flag("source-config", "Configuration file for the source repository").Required().ExistingFileVar(&c.migrateSourceConfig)
	cmd.Flag("sources", "List of sources to migrate").StringsVar(&c.migrateSources)
	cmd.Flag("all", "Migrate all sources").BoolVar(&c.migrateAll)
	cmd.Flag("policies", "Migrate policies too").Default("true").BoolVar(&c.migratePolicies)
	cmd.Flag("overwrite-policies", "Overwrite policies").BoolVar(&c.migrateOverwritePolicies)
	cmd.Flag("latest-only", "Only migrate the latest snapshot").BoolVar(&c.migrateLatestOnly)
	cmd.Flag("parallel", "Number of sources to migrate in parallel").Default("1").IntVar(&c.migrateParallel)
	cmd.Flag("apply-ignore-rules", "When migrating also apply current ignore rules").BoolVar(&c.applyIgnoreRules)
	cmd.Action(svc.repositoryWriterAction(c.run))

	c.svc = svc
	c.out.setup(svc)
}

func (c *commandSnapshotMigrate) run(ctx context.Context, destRepo repo.RepositoryWriter) error {
	sourceRepo, err := c.openSourceRepo(ctx)
	if err != nil {
		return errors.Wrap(err, "can't open source repository")
	}

	defer sourceRepo.Close(ctx) //nolint:errcheck

	sources, err := c.getSourcesToMigrate(ctx, sourceRepo)
	if err != nil {
		return errors.Wrap(err, "can't retrieve sources")
	}

	semaphore := make(chan struct{}, c.migrateParallel)

	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		canceled        bool
		activeUploaders = map[snapshot.SourceInfo]*snapshotfs.Uploader{}
	)

	c.svc.getProgress().StartShared()

	c.svc.onTerminate(func() {
		mu.Lock()
		defer mu.Unlock()

		if canceled {
			return
		}

		canceled = true

		for s, u := range activeUploaders {
			log(ctx).Infof("canceling active uploader for %v", s)
			u.Cancel()
		}
	})

	if c.migratePolicies {
		if c.migrateAll {
			err = c.migrateAllPolicies(ctx, sourceRepo, destRepo)
		} else {
			err = c.migratePoliciesForSources(ctx, sourceRepo, destRepo, sources)
		}

		if err != nil {
			return errors.Wrap(err, "unable to migrate policies")
		}
	}

	for _, s := range sources {
		// start a new uploader unless already canceled
		mu.Lock()
		if canceled {
			mu.Unlock()
			break
		}

		uploader := snapshotfs.NewUploader(destRepo)
		uploader.Progress = c.svc.getProgress()
		activeUploaders[s] = uploader
		mu.Unlock()

		wg.Add(1)
		semaphore <- struct{}{}

		go func(s snapshot.SourceInfo) {
			defer func() {
				mu.Lock()
				delete(activeUploaders, s)
				mu.Unlock()

				<-semaphore
				wg.Done()
			}()

			if err := c.migrateSingleSource(ctx, uploader, sourceRepo, destRepo, s); err != nil {
				log(ctx).Errorf("unable to migrate source: %v", err)
			}
		}(s)
	}

	wg.Wait()
	c.svc.getProgress().FinishShared()
	c.out.printStderr("\r\n")
	log(ctx).Info("Migration finished.")

	return nil
}

func (c *commandSnapshotMigrate) openSourceRepo(ctx context.Context) (repo.Repository, error) {
	pass, err := c.svc.passwordPersistenceStrategy().GetPassword(ctx, c.migrateSourceConfig)
	if err != nil {
		pass, err = c.svc.getPasswordFromFlags(ctx, false, false)
	}

	if err != nil {
		return nil, errors.Wrap(err, "source repository password")
	}

	sourceRepo, err := repo.Open(ctx, c.migrateSourceConfig, pass, c.svc.optionsFromFlags(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "can't open source repository")
	}

	return sourceRepo, nil
}

func (c *commandSnapshotMigrate) migratePoliciesForSources(ctx context.Context, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, sources []snapshot.SourceInfo) error {
	for _, si := range sources {
		if err := c.migrateSinglePolicy(ctx, sourceRepo, destRepo, si); err != nil {
			return errors.Wrapf(err, "unable to migrate policy for %v", si)
		}
	}

	return nil
}

func (c *commandSnapshotMigrate) migrateAllPolicies(ctx context.Context, sourceRepo repo.Repository, destRepo repo.RepositoryWriter) error {
	policies, err := policy.ListPolicies(ctx, sourceRepo)
	if err != nil {
		return errors.Wrap(err, "unable to list source policies")
	}

	for _, pol := range policies {
		if err := c.migrateSinglePolicy(ctx, sourceRepo, destRepo, pol.Target()); err != nil {
			log(ctx).Errorf("unable to migrate policy for %v: %v", pol.Target(), err)
		}
	}

	return nil
}

func (c *commandSnapshotMigrate) migrateSinglePolicy(ctx context.Context, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, si snapshot.SourceInfo) error {
	pol, err := policy.GetDefinedPolicy(ctx, sourceRepo, si)
	if errors.Is(err, policy.ErrPolicyNotFound) {
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "unable to migrate policy for %v", si)
	}

	_, err = policy.GetDefinedPolicy(ctx, destRepo, si)
	if err == nil {
		if !c.migrateOverwritePolicies {
			log(ctx).Infof("policy already set for %v", si)
			// already have destination policy
			return nil
		}
	} else if !errors.Is(err, policy.ErrPolicyNotFound) {
		return errors.Wrapf(err, "unable to migrate policy for %v", si)
	}

	log(ctx).Infof("migrating policy for %v", si)

	return errors.Wrap(policy.SetPolicy(ctx, destRepo, si, pol), "error setting policy")
}

func (c *commandSnapshotMigrate) findPreviousSnapshotManifestWithStartTime(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, startTime fs.UTCTimestamp) (*snapshot.Manifest, error) {
	previous, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous snapshots")
	}

	for _, p := range previous {
		if p.StartTime.Equal(startTime) {
			return p, nil
		}
	}

	return nil, nil
}

func (c *commandSnapshotMigrate) migrateSingleSource(ctx context.Context, uploader *snapshotfs.Uploader, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, s snapshot.SourceInfo) error {
	manifests, err := snapshot.ListSnapshotManifests(ctx, sourceRepo, &s, nil)
	if err != nil {
		return errors.Wrapf(err, "error listing snapshot manifests for %v", s)
	}

	snapshots, err := snapshot.LoadSnapshots(ctx, sourceRepo, manifests)
	if err != nil {
		return errors.Wrapf(err, "unable to load snapshot manifests for %v", s)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].StartTime.Before(snapshots[j].StartTime)
	})

	for _, m := range c.filterSnapshotsToMigrate(snapshots) {
		if uploader.IsCanceled() {
			break
		}

		if err := c.migrateSingleSourceSnapshot(ctx, uploader, sourceRepo, destRepo, s, m); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandSnapshotMigrate) migrateSingleSourceSnapshot(ctx context.Context, uploader *snapshotfs.Uploader, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, s snapshot.SourceInfo, m *snapshot.Manifest) error {
	if m.IncompleteReason != "" {
		log(ctx).Debugf("ignoring incomplete %v at %v", s, formatTimestamp(m.StartTime.ToTime()))
		return nil
	}

	sourceEntry, err := snapshotfs.SnapshotRoot(sourceRepo, m)
	if err != nil {
		return errors.Wrap(err, "error getting snapshot root entry")
	}

	existing, err := c.findPreviousSnapshotManifestWithStartTime(ctx, destRepo, m.Source, m.StartTime)
	if err != nil {
		return err
	}

	if existing != nil {
		log(ctx).Infof("already migrated %v at %v", s, formatTimestamp(m.StartTime.ToTime()))
		return nil
	}

	log(ctx).Infof("migrating snapshot of %v at %v", s, formatTimestamp(m.StartTime.ToTime()))

	previous, err := findPreviousSnapshotManifest(ctx, destRepo, m.Source, &m.StartTime)
	if err != nil {
		return err
	}

	policyTree, err := policy.TreeForSource(ctx, destRepo, m.Source)
	if err != nil {
		return errors.Wrap(err, "error generating policy tree")
	}

	uploader.DisableIgnoreRules = !c.applyIgnoreRules

	newm, err := uploader.Upload(ctx, sourceEntry, policyTree, m.Source, previous...)
	if err != nil {
		return errors.Wrapf(err, "error migrating snapshot %v @ %v", m.Source, m.StartTime)
	}

	newm.StartTime = m.StartTime
	newm.EndTime = m.EndTime
	newm.Description = m.Description

	if newm.IncompleteReason == "" {
		if _, err := snapshot.SaveSnapshot(ctx, destRepo, newm); err != nil {
			return errors.Wrap(err, "cannot save manifest")
		}
	}

	return nil
}

func (c *commandSnapshotMigrate) filterSnapshotsToMigrate(s []*snapshot.Manifest) []*snapshot.Manifest {
	if c.migrateLatestOnly && len(s) > 0 {
		s = s[len(s)-1:]
	}

	return s
}

func (c *commandSnapshotMigrate) getSourcesToMigrate(ctx context.Context, rep repo.Repository) ([]snapshot.SourceInfo, error) {
	if len(c.migrateSources) > 0 {
		var result []snapshot.SourceInfo

		for _, s := range c.migrateSources {
			si, err := snapshot.ParseSourceInfo(s, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse %q", s)
			}

			result = append(result, si)
		}

		return result, nil
	}

	if c.migrateAll {
		//nolint:wrapcheck
		return snapshot.ListSources(ctx, rep)
	}

	return nil, errors.New("must specify either --all or --sources")
}
