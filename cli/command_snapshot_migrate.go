package cli

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	migrateCommand           = snapshotCommands.Command("migrate", "Migrate snapshots from another repository")
	migrateSourceConfig      = migrateCommand.Flag("source-config", "Configuration file for the source repository").Required().ExistingFile()
	migrateSources           = migrateCommand.Flag("sources", "List of sources to migrate").Strings()
	migrateAll               = migrateCommand.Flag("all", "Migrate all sources").Bool()
	migratePolicies          = migrateCommand.Flag("policies", "Migrate policies too").Default("true").Bool()
	migrateOverwritePolicies = migrateCommand.Flag("overwrite-policies", "Overwrite policies").Bool()
	migrateLatestOnly        = migrateCommand.Flag("latest-only", "Only migrate the latest snapshot").Bool()
	migrateParallel          = migrateCommand.Flag("parallel", "Number of sources to migrate in parallel").Default("1").Int()
)

func runMigrateCommand(ctx context.Context, destRepo repo.RepositoryWriter) error {
	sourceRepo, err := openSourceRepo(ctx)
	if err != nil {
		return errors.Wrap(err, "can't open source repository")
	}

	sources, err := getSourcesToMigrate(ctx, sourceRepo)
	if err != nil {
		return errors.Wrap(err, "can't retrieve sources")
	}

	semaphore := make(chan struct{}, *migrateParallel)

	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		canceled        bool
		activeUploaders = map[snapshot.SourceInfo]*snapshotfs.Uploader{}
	)

	progress.StartShared()

	onCtrlC(func() {
		mu.Lock()
		defer mu.Unlock()

		if !canceled {
			canceled = true
			for s, u := range activeUploaders {
				log(ctx).Infof("canceling active uploader for %v", s)
				u.Cancel()
			}
		}
	})

	if *migratePolicies {
		if *migrateAll {
			err = migrateAllPolicies(ctx, sourceRepo, destRepo)
		} else {
			err = migratePoliciesForSources(ctx, sourceRepo, destRepo, sources)
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
		uploader.Progress = progress
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

			if err := migrateSingleSource(ctx, uploader, sourceRepo, destRepo, s); err != nil {
				log(ctx).Errorf("unable to migrate source: %v", err)
			}
		}(s)
	}

	wg.Wait()
	progress.FinishShared()
	printStderr("\r\n")
	log(ctx).Infof("Migration finished.")

	return nil
}

func openSourceRepo(ctx context.Context) (repo.Repository, error) {
	pass, ok := repo.GetPersistedPassword(ctx, *migrateSourceConfig)
	if !ok {
		var err error

		if pass, err = getPasswordFromFlags(ctx, false, false); err != nil {
			return nil, errors.Wrap(err, "source repository password")
		}
	}

	sourceRepo, err := repo.Open(ctx, *migrateSourceConfig, pass, optionsFromFlags(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "can't open source repository")
	}

	return sourceRepo, nil
}

func migratePoliciesForSources(ctx context.Context, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, sources []snapshot.SourceInfo) error {
	for _, si := range sources {
		if err := migrateSinglePolicy(ctx, sourceRepo, destRepo, si); err != nil {
			return errors.Wrapf(err, "unable to migrate policy for %v", si)
		}
	}

	return nil
}

func migrateAllPolicies(ctx context.Context, sourceRepo repo.Repository, destRepo repo.RepositoryWriter) error {
	policies, err := policy.ListPolicies(ctx, sourceRepo)
	if err != nil {
		return errors.Wrap(err, "unable to list source policies")
	}

	for _, pol := range policies {
		if err := migrateSinglePolicy(ctx, sourceRepo, destRepo, pol.Target()); err != nil {
			log(ctx).Errorf("unable to migrate policy for %v: %v", pol.Target(), err)
		}
	}

	return nil
}

func migrateSinglePolicy(ctx context.Context, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, si snapshot.SourceInfo) error {
	pol, err := policy.GetDefinedPolicy(ctx, sourceRepo, si)
	if errors.Is(err, policy.ErrPolicyNotFound) {
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "unable to migrate policy for %v", si)
	}

	_, err = policy.GetDefinedPolicy(ctx, destRepo, si)
	if err == nil {
		if !*migrateOverwritePolicies {
			log(ctx).Infof("policy already set for %v", si)
			// already have destination policy
			return nil
		}
	} else if !errors.Is(err, policy.ErrPolicyNotFound) {
		return errors.Wrapf(err, "unable to migrate policy for %v", si)
	}

	log(ctx).Infof("migrating policy for %v", si)

	return policy.SetPolicy(ctx, destRepo, si, pol)
}

func findPreviousSnapshotManifestWithStartTime(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, startTime time.Time) (*snapshot.Manifest, error) {
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

func migrateSingleSource(ctx context.Context, uploader *snapshotfs.Uploader, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, s snapshot.SourceInfo) error {
	manifests, err := snapshot.ListSnapshotManifests(ctx, sourceRepo, &s)
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

	for _, m := range filterSnapshotsToMigrate(snapshots) {
		if uploader.IsCanceled() {
			break
		}

		if err := migrateSingleSourceSnapshot(ctx, uploader, sourceRepo, destRepo, s, m); err != nil {
			return err
		}
	}

	return nil
}

func migrateSingleSourceSnapshot(ctx context.Context, uploader *snapshotfs.Uploader, sourceRepo repo.Repository, destRepo repo.RepositoryWriter, s snapshot.SourceInfo, m *snapshot.Manifest) error {
	if m.IncompleteReason != "" {
		log(ctx).Debugf("ignoring incomplete %v at %v", s, formatTimestamp(m.StartTime))
		return nil
	}

	sourceEntry, err := snapshotfs.SnapshotRoot(sourceRepo, m)
	if err != nil {
		return errors.Wrap(err, "error getting snapshot root entry")
	}

	existing, err := findPreviousSnapshotManifestWithStartTime(ctx, destRepo, m.Source, m.StartTime)
	if err != nil {
		return err
	}

	if existing != nil {
		log(ctx).Infof("already migrated %v at %v", s, formatTimestamp(m.StartTime))
		return nil
	}

	log(ctx).Infof("migrating snapshot of %v at %v", s, formatTimestamp(m.StartTime))

	previous, err := findPreviousSnapshotManifest(ctx, destRepo, m.Source, &m.StartTime)
	if err != nil {
		return err
	}

	var policyTree *policy.Tree

	newm, err := uploader.Upload(ctx, sourceEntry, policyTree, m.Source, previous...)
	if err != nil {
		return errors.Wrapf(err, "error migrating shapshot %v @ %v", m.Source, m.StartTime)
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

func filterSnapshotsToMigrate(s []*snapshot.Manifest) []*snapshot.Manifest {
	if *migrateLatestOnly && len(s) > 0 {
		s = s[0:1]
	}

	return s
}

func getSourcesToMigrate(ctx context.Context, rep repo.Repository) ([]snapshot.SourceInfo, error) {
	if len(*migrateSources) > 0 {
		var result []snapshot.SourceInfo

		for _, s := range *migrateSources {
			si, err := snapshot.ParseSourceInfo(s, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse %q", s)
			}

			result = append(result, si)
		}

		return result, nil
	}

	if *migrateAll {
		return snapshot.ListSources(ctx, rep)
	}

	return nil, errors.New("must specify either --all or --sources")
}

func init() {
	migrateCommand.Action(repositoryWriterAction(runMigrateCommand))
}
