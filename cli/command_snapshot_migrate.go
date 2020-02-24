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
	migrateCommand      = snapshotCommands.Command("migrate", "Migrate snapshots from another repository")
	migrateSourceConfig = migrateCommand.Flag("source-config", "Configuration file for the source repository").Required().ExistingFile()
	migrateSources      = migrateCommand.Flag("sources", "List of sources to migrate").Strings()
	migrateAll          = migrateCommand.Flag("all", "Migrate all sources").Bool()
	migrateLatestOnly   = migrateCommand.Flag("latest-only", "Only migrate the latest snapshot").Bool()
	migrateIgnoreErrors = migrateCommand.Flag("ignore-errors", "Ignore errors when reading source backup").Bool()
	migrateParallelism  = migrateCommand.Flag("parallelism", "Number of sources to migrate in parallel").Default("1").Int()
)

func runMigrateCommand(ctx context.Context, destRepo *repo.Repository) error {
	pass, err := getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "source repository password")
	}

	sourceRepo, err := repo.Open(ctx, *migrateSourceConfig, pass, applyOptionsFromFlags(ctx, nil))
	if err != nil {
		return errors.Wrap(err, "can't open source repository")
	}

	sources, err := getSourcesToMigrate(ctx, sourceRepo)
	if err != nil {
		return errors.Wrap(err, "can't retrieve sources")
	}

	semaphore := make(chan struct{}, *migrateParallelism)

	var (
		wg              sync.WaitGroup
		mu              sync.Mutex
		canceled        bool
		activeUploaders = map[snapshot.SourceInfo]*snapshotfs.Uploader{}
	)

	onCtrlC(func() {
		mu.Lock()
		defer mu.Unlock()

		if !canceled {
			canceled = true
			for s, u := range activeUploaders {
				log(ctx).Warningf("canceling active uploader for %v", s)
				u.Cancel()
			}
		}
	})

	for _, s := range sources {
		// start a new uploader unless already canceled
		mu.Lock()
		if canceled {
			mu.Unlock()
			break
		}

		uploader := snapshotfs.NewUploader(destRepo)
		uploader.Progress = progress
		uploader.IgnoreReadErrors = *migrateIgnoreErrors
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
				log(ctx).Warningf("unable to migrate source: %v", err)
			}
		}(s)
	}

	wg.Wait()

	return nil
}

func findPreviousSnapshotManifestWithStartTime(ctx context.Context, rep *repo.Repository, sourceInfo snapshot.SourceInfo, startTime time.Time) (*snapshot.Manifest, error) {
	previous, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous backups")
	}

	for _, p := range previous {
		if p.StartTime == startTime {
			return p, nil
		}
	}

	return nil, nil
}

func migrateSingleSource(ctx context.Context, uploader *snapshotfs.Uploader, sourceRepo, destRepo *repo.Repository, s snapshot.SourceInfo) error {
	manifests, err := snapshot.ListSnapshotManifests(ctx, sourceRepo, &s)
	if err != nil {
		return err
	}

	snapshots, err := snapshot.LoadSnapshots(ctx, sourceRepo, manifests)
	if err != nil {
		return errors.Wrapf(err, "unable to load snapshot manifests for %v", s)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].StartTime.Before(snapshots[j].StartTime)
	})

	for _, m := range filterSnapshotsToMigrate(snapshots) {
		if uploader.IsCancelled() {
			break
		}

		if err := migrateSingleSourceSnapshot(ctx, uploader, sourceRepo, destRepo, s, m); err != nil {
			return err
		}
	}

	return nil
}

func migrateSingleSourceSnapshot(ctx context.Context, uploader *snapshotfs.Uploader, sourceRepo, destRepo *repo.Repository, s snapshot.SourceInfo, m *snapshot.Manifest) error {
	if m.IncompleteReason != "" {
		log(ctx).Infof("ignoring incomplete %v at %v", s, formatTimestamp(m.StartTime))
		return nil
	}

	sourceEntry := snapshotfs.DirectoryEntry(sourceRepo, m.RootObjectID(), nil)

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

func getSourcesToMigrate(ctx context.Context, rep *repo.Repository) ([]snapshot.SourceInfo, error) {
	if len(*migrateSources) > 0 {
		var result []snapshot.SourceInfo

		for _, s := range *migrateSources {
			si, err := snapshot.ParseSourceInfo(s, rep.Hostname, rep.Username)
			if err != nil {
				return nil, err
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
	migrateCommand.Action(repositoryAction(runMigrateCommand))
}
