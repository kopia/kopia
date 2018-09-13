package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/fs/repofs"
	"github.com/kopia/kopia/internal/upload"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

var (
	migrateCommand      = repositoryCommands.Command("migrate", "Migrate data from old repository to a new one.")
	migrateSourceConfig = migrateCommand.Flag("source-config", "Configuration file for the source repository").Required().ExistingFile()
	migrateSources      = migrateCommand.Flag("sources", "List of sources to migrate").Strings()
	migrateAll          = migrateCommand.Flag("all", "Migrate all sources").Bool()
	migrateDirectories  = migrateCommand.Flag("directory-objects", "Migrate directory objects").Strings()
	migrateLatestOnly   = migrateCommand.Flag("latest-only", "Only migrate the latest snapshot").Bool()
	migrateIgnoreErrors = migrateCommand.Flag("ignore-errors", "Ignore errors when reading source backup").Bool()
)

func runMigrateCommand(ctx context.Context, destRepo *repo.Repository) error {
	uploader := upload.NewUploader(destRepo)
	uploader.Progress = &uploadProgress{}
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

	for _, dir := range *migrateDirectories {
		dirOID, err := object.ParseID(dir)
		if err != nil {
			return err
		}
		d := repofs.DirectoryEntry(sourceRepo, dirOID, nil)
		newm, err := uploader.Upload(ctx, d, snapshot.SourceInfo{Host: "temp"}, nil)
		if err != nil {
			return fmt.Errorf("error migrating directory %v: %v", dirOID, err)
		}

		log.Debugf("migrated directory: %v with %#v", dirOID, newm)
	}

	return nil
}

func migrateSingleSource(ctx context.Context, uploader *upload.Uploader, sourceRepo, destRepo *repo.Repository, s snapshot.SourceInfo) error {
	log.Debugf("migrating source %v", s)

	manifests, err := snapshot.ListSnapshotManifests(ctx, sourceRepo, &s)
	if err != nil {
		return err
	}
	snapshots, err := snapshot.LoadSnapshots(ctx, sourceRepo, manifests)
	if err != nil {
		return fmt.Errorf("unable to load snapshot manifests for %v: %v", s, err)
	}

	for _, m := range filterSnapshotsToMigrate(snapshots) {
		d := repofs.DirectoryEntry(sourceRepo, m.RootObjectID(), nil)
		newm, err := uploader.Upload(ctx, d, m.Source, nil)
		if err != nil {
			return fmt.Errorf("error migrating shapshot %v @ %v: %v", m.Source, m.StartTime, err)
		}

		m.RootEntry = newm.RootEntry
		m.HashCacheID = newm.HashCacheID
		m.Stats = newm.Stats
		m.IncompleteReason = newm.IncompleteReason

		if _, err := snapshot.SaveSnapshot(ctx, destRepo, m); err != nil {
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

	return nil, nil
}

func init() {
	migrateCommand.Action(repositoryAction(runMigrateCommand))
}
