package cli

import (
	"fmt"
	"log"

	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
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

func runMigrateCommand(context *kingpin.ParseContext) error {
	destRepo := mustOpenRepository(nil)
	destSM := snapshot.NewManager(destRepo)

	uploader := snapshot.NewUploader(destRepo)
	uploader.Progress = &uploadProgress{}
	uploader.IgnoreFileErrors = *migrateIgnoreErrors
	onCtrlC(uploader.Cancel)

	sourceRepo, err := repo.Open(getContext(), *migrateSourceConfig, applyOptionsFromFlags(nil))
	if err != nil {
		return fmt.Errorf("can't open source repository: %v", err)
	}

	sourceSM := snapshot.NewManager(sourceRepo)
	sources, err := getSourcesToMigrate(sourceSM)
	if err != nil {
		return fmt.Errorf("can't retrieve sources: %v", err)
	}

	for _, s := range sources {
		if uploader.IsCancelled() {
			log.Printf("upload cancelled")
			break
		}

		log.Printf("migrating source %v", s)

		manifests, err := sourceSM.ListSnapshotManifests(s)
		if err != nil {
			return fmt.Errorf("unable to list snapshot manifests for %v: %v", s, err)
		}

		snapshots, err := sourceSM.LoadSnapshots(manifests)
		if err != nil {
			return fmt.Errorf("unable to load snapshot manifests for %v: %v", s, err)
		}

		for _, m := range filterSnapshotsToMigrate(snapshots) {
			d := sourceSM.DirectoryEntry(m.RootObjectID)
			newm, err := uploader.Upload(d, &m.Source, nil)
			if err != nil {
				return fmt.Errorf("error migrating shapshot %v @ %v: %v", m.Source, m.StartTime, err)
			}

			m.RootObjectID = newm.RootObjectID
			m.HashCacheID = newm.HashCacheID
			m.Stats = newm.Stats
			m.IncompleteReason = newm.IncompleteReason

			if _, err := destSM.SaveSnapshot(m); err != nil {
				return fmt.Errorf("cannot save manifest: %v", err)
			}
		}
	}

	for _, dir := range *migrateDirectories {
		dirOID, err := object.ParseID(dir)
		if err != nil {
			return err
		}
		d := sourceSM.DirectoryEntry(dirOID)
		newm, err := uploader.Upload(d, &snapshot.SourceInfo{Host: "temp"}, nil)
		if err != nil {
			return fmt.Errorf("error migrating directory %v: %v", dirOID, err)
		}

		log.Printf("migrated directory: %v with %#v", dirOID, newm)
	}

	return nil
}

func filterSnapshotsToMigrate(s []*snapshot.Manifest) []*snapshot.Manifest {
	if *migrateLatestOnly && len(s) > 0 {
		s = s[0:1]
	}
	return s
}

func getSourcesToMigrate(mgr *snapshot.Manager) ([]*snapshot.SourceInfo, error) {
	if len(*migrateSources) > 0 {
		var result []*snapshot.SourceInfo

		for _, s := range *migrateSources {
			si, err := snapshot.ParseSourceInfo(s, getHostName(), getUserName())
			if err != nil {
				return nil, err
			}

			result = append(result, &si)
		}

		return result, nil
	}

	if *migrateAll {
		return mgr.ListSources()
	}

	return nil, nil
}

func init() {
	migrateCommand.Action(runMigrateCommand)
}
