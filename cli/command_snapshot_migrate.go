package cli

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/repo"
)

var (
	migrateCommand      = snapshotCommands.Command("migrate", "Migrate snapshots from another repository")
	migrateSourceConfig = migrateCommand.Flag("source-config", "Configuration file for the source repository").Required().ExistingFile()
	migrateSources      = migrateCommand.Flag("sources", "List of sources to migrate").Strings()
	migrateAll          = migrateCommand.Flag("all", "Migrate all sources").Bool()
	migrateLatestOnly   = migrateCommand.Flag("latest-only", "Only migrate the latest snapshot").Bool()
	migrateIgnoreErrors = migrateCommand.Flag("ignore-errors", "Ignore errors when reading source backup").Bool()
)

func runMigrateCommand(ctx context.Context, destRepo *repo.Repository) error {
	uploader := snapshotfs.NewUploader(destRepo)
	uploader.Progress = cliProgress
	uploader.IgnoreFileErrors = *migrateIgnoreErrors
	onCtrlC(uploader.Cancel)

	sourceRepo, err := repo.Open(ctx, *migrateSourceConfig, mustGetPasswordFromFlags(false, false), applyOptionsFromFlags(nil))
	if err != nil {
		return fmt.Errorf("can't open source repository: %v", err)
	}

	sources, err := getSourcesToMigrate(ctx, sourceRepo)
	if err != nil {
		return fmt.Errorf("can't retrieve sources: %v", err)
	}

	for _, s := range sources {
		if uploader.IsCancelled() {
			log.Debugf("upload cancelled")
			break
		}

		if err := migrateSingleSource(ctx, uploader, sourceRepo, destRepo, s); err != nil {
			return err
		}
	}

	return nil
}

func findPreviousSnapshotManifestWithStartTime(ctx context.Context, rep *repo.Repository, sourceInfo snapshot.SourceInfo, startTime time.Time) (*snapshot.Manifest, error) {
	previous, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, fmt.Errorf("error listing previous backups: %v", err)
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
		return fmt.Errorf("unable to load snapshot manifests for %v: %v", s, err)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].StartTime.Before(snapshots[j].StartTime)
	})

	for _, m := range filterSnapshotsToMigrate(snapshots) {
		sourceEntry := snapshotfs.DirectoryEntry(sourceRepo, m.RootObjectID(), nil)

		existing, err := findPreviousSnapshotManifestWithStartTime(ctx, destRepo, m.Source, m.StartTime)
		if err != nil {
			return err
		}
		if existing != nil {
			log.Infof("already migrated %v at %v", s, formatTimestamp(m.StartTime))
			continue
		}

		log.Infof("migrating snapshot of %v at %v", s, formatTimestamp(m.StartTime))
		previousManifest, err := findPreviousSnapshotManifest(ctx, destRepo, m.Source, &m.StartTime)
		if err != nil {
			return err
		}

		newm, err := uploader.Upload(ctx, sourceEntry, m.Source, previousManifest)
		if err != nil {
			return fmt.Errorf("error migrating shapshot %v @ %v: %v", m.Source, m.StartTime, err)
		}

		newm.StartTime = m.StartTime
		newm.EndTime = m.EndTime
		newm.Description = m.Description

		if _, err := snapshot.SaveSnapshot(ctx, destRepo, newm); err != nil {
			return fmt.Errorf("cannot save manifest: %v", err)
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
			si, err := snapshot.ParseSourceInfo(s, getHostName(), getUserName())
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

	return nil, fmt.Errorf("must specify either --all or --sources")
}

func init() {
	migrateCommand.Action(repositoryAction(runMigrateCommand))
}
