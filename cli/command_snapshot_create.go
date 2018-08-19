package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
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
	mgr := snapshot.NewManager(rep)
	pmgr := snapshot.NewPolicyManager(rep)

	sources := *snapshotCreateSources
	if *snapshotCreateAll {
		local, err := getLocalBackupPaths(mgr)
		if err != nil {
			return err
		}
		sources = append(sources, local...)
	}

	if len(sources) == 0 {
		return errors.New("no backup sources")
	}

	u := snapshot.NewUploader(rep)
	u.MaxUploadBytes = *snapshotCreateCheckpointUploadLimitMB * 1024 * 1024
	u.ForceHashPercentage = *snapshotCreateForceHash
	u.HashCacheMinAge = *snapshotCreateHashCacheMinAge
	u.ParallelUploads = *snapshotCreateParallelUploads
	onCtrlC(u.Cancel)

	u.Progress = &uploadProgress{}

	if len(*snapshotCreateDescription) > maxSnapshotDescriptionLength {
		return fmt.Errorf("description too long")
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
		if err := snapshotSingleSource(ctx, rep, mgr, pmgr, u, sourceInfo); err != nil {
			finalErrors = append(finalErrors, err.Error())
		}
	}

	if len(finalErrors) == 0 {
		return nil
	}

	return fmt.Errorf("encountered %v errors:\n%v", len(finalErrors), strings.Join(finalErrors, "\n"))
}

func snapshotSingleSource(ctx context.Context, rep *repo.Repository, mgr *snapshot.Manager, pmgr *snapshot.PolicyManager, u *snapshot.Uploader, sourceInfo snapshot.SourceInfo) error {
	t0 := time.Now()
	rep.Blocks.ResetStats()

	localEntry := mustGetLocalFSEntry(sourceInfo.Path)

	previousManifest, err := findPreviousSnapshotManifest(mgr, sourceInfo)
	if err != nil {
		return err
	}

	u.FilesPolicy, err = pmgr.FilesPolicyGetter(sourceInfo)
	if err != nil {
		return err
	}

	log.Debugf("uploading %v using previous manifest %v", sourceInfo, previousManifest)
	manifest, err := u.Upload(ctx, localEntry, sourceInfo, previousManifest)
	if err != nil {
		return err
	}

	manifest.Description = *snapshotCreateDescription

	snapID, err := mgr.SaveSnapshot(manifest)
	if err != nil {
		return fmt.Errorf("cannot save manifest: %v", err)
	}

	log.Infof("uploaded snapshot %v (root %v) in %v", snapID, manifest.RootObjectID(), time.Since(t0))
	log.Debugf("Hash Cache: %v", manifest.HashCacheID.String())

	b, _ := json.MarshalIndent(&manifest, "", "  ")
	log.Debugf("%s", string(b))

	return nil
}

func findPreviousSnapshotManifest(mgr *snapshot.Manager, sourceInfo snapshot.SourceInfo) (*snapshot.Manifest, error) {
	previous, err := mgr.ListSnapshots(sourceInfo)
	if err != nil {
		return nil, fmt.Errorf("error listing previous backups: %v", err)
	}

	var previousManifest *snapshot.Manifest
	for _, p := range previous {
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

func getLocalBackupPaths(mgr *snapshot.Manager) ([]string, error) {
	h := getHostName()
	u := getUserName()
	log.Debugf("Looking for previous backups of '%v@%v'...", u, h)

	sources := mgr.ListSources()

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
