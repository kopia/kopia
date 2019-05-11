package cli

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/repo"
	"github.com/pkg/errors"
)

const (
	maxSnapshotDescriptionLength = 1024
)

var (
	snapshotCreateCommand = snapshotCommands.Command("create", "Creates a snapshot of local directory or file.").Default()

	snapshotCreateSources                 = snapshotCreateCommand.Arg("source", "Files or directories to create snapshot(s) of.").ExistingFilesOrDirs()
	snapshotCreateAll                     = snapshotCreateCommand.Flag("all", "Create snapshots for files or directories previously backed up by this user on this computer").Bool()
	snapshotCreateCheckpointUploadLimitMB = snapshotCreateCommand.Flag("upload-limit-mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int64()
	snapshotCreateDescription             = snapshotCreateCommand.Flag("description", "Free-form snapshot description.").String()
	snapshotCreateForceHash               = snapshotCreateCommand.Flag("force-hash", "Force hashing of source files for a given percentage of files [0..100]").Default("0").Int()
	snapshotCreateHashCacheMinAge         = snapshotCreateCommand.Flag("hash-cache-min-age", "Do not hash-cache files below certain age").Default("10m").Duration()
	snapshotCreateParallelUploads         = snapshotCreateCommand.Flag("parallel", "Upload N files in parallel").PlaceHolder("N").Default("0").Int()
)

func runBackupCommand(ctx context.Context, rep *repo.Repository) error {
	sources := *snapshotCreateSources
	if *snapshotCreateAll {
		local, err := getLocalBackupPaths(ctx, rep)
		if err != nil {
			return err
		}
		sources = append(sources, local...)
	}

	if len(sources) == 0 {
		return errors.New("no backup sources")
	}

	u := snapshotfs.NewUploader(rep)
	u.MaxUploadBytes = *snapshotCreateCheckpointUploadLimitMB * 1024 * 1024
	u.ForceHashPercentage = *snapshotCreateForceHash
	u.HashCacheMinAge = *snapshotCreateHashCacheMinAge
	u.ParallelUploads = *snapshotCreateParallelUploads
	onCtrlC(u.Cancel)

	u.Progress = cliProgress

	if len(*snapshotCreateDescription) > maxSnapshotDescriptionLength {
		return errors.New("description too long")
	}

	var finalErrors []string

	for _, snapshotDir := range sources {
		log.Debugf("Backing up %v", snapshotDir)
		dir, err := filepath.Abs(snapshotDir)
		if err != nil {
			return fmt.Errorf("invalid source: '%s': %s", snapshotDir, err)
		}

		sourceInfo := snapshot.SourceInfo{Path: filepath.Clean(dir), Host: getHostName(), UserName: getUserName()}
		log.Infof("snapshotting %v", sourceInfo)
		if err := snapshotSingleSource(ctx, rep, u, sourceInfo); err != nil {
			finalErrors = append(finalErrors, err.Error())
		}
	}

	if len(finalErrors) == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors:\n%v", len(finalErrors), strings.Join(finalErrors, "\n"))
}

func snapshotSingleSource(ctx context.Context, rep *repo.Repository, u *snapshotfs.Uploader, sourceInfo snapshot.SourceInfo) error {
	t0 := time.Now()
	rep.Blocks.ResetStats()

	localEntry := mustGetLocalFSEntry(sourceInfo.Path)

	previousManifest, err := findPreviousSnapshotManifest(ctx, rep, sourceInfo, nil)
	if err != nil {
		return err
	}

	u.FilesPolicy, err = policy.FilesPolicyGetter(ctx, rep, sourceInfo)
	if err != nil {
		return err
	}

	log.Debugf("uploading %v using previous manifest %v", sourceInfo, previousManifest)
	manifest, err := u.Upload(ctx, localEntry, sourceInfo, previousManifest)
	if err != nil {
		return err
	}

	manifest.Description = *snapshotCreateDescription

	snapID, err := snapshot.SaveSnapshot(ctx, rep, manifest)
	if err != nil {
		return errors.Wrap(err, "cannot save manifest")
	}

	printStderr("uploaded snapshot %v (root %v) in %v\n", snapID, manifest.RootObjectID(), time.Since(t0))
	log.Debugf("Hash Cache: %v", manifest.HashCacheID.String())

	_, err = policy.ApplyRetentionPolicy(ctx, rep, sourceInfo, true)
	return err
}

func findPreviousSnapshotManifest(ctx context.Context, rep *repo.Repository, sourceInfo snapshot.SourceInfo, noLaterThan *time.Time) (*snapshot.Manifest, error) {
	previous, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous backups")
	}

	var previousManifest *snapshot.Manifest
	for _, p := range previous {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}
		if previousManifest == nil || p.StartTime.After(previousManifest.StartTime) {
			previousManifest = p
		}
	}

	if previousManifest != nil {
		log.Debugf("found previous manifest for %v with start time %v", sourceInfo, previousManifest.StartTime)
	} else {
		log.Debugf("no previous manifest for %v", sourceInfo)
	}

	return previousManifest, nil
}

func getLocalBackupPaths(ctx context.Context, rep *repo.Repository) ([]string, error) {
	h := getHostName()
	u := getUserName()
	log.Debugf("Looking for previous backups of '%v@%v'...", u, h)

	sources, err := snapshot.ListSources(ctx, rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list sources")
	}

	var result []string

	for _, src := range sources {
		if src.Host == h && src.UserName == u {
			result = append(result, src.Path)
		}
	}

	return result, nil
}

func getUserName() string {
	currentUser, err := user.Current()
	if err != nil {
		log.Warningf("Cannot determine current user: %s", err)
		return "nobody"
	}

	u := currentUser.Username
	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u
}

func getHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Warningf("Unable to determine hostname: %s", err)
		return "nohost"
	}

	// Normalize hostname.
	hostname = strings.ToLower(strings.Split(hostname, ".")[0])

	return hostname
}

func init() {
	snapshotCreateCommand.Action(repositoryAction(runBackupCommand))
}
