package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	maxSnapshotDescriptionLength = 1024
	timeFormat                   = "2006-01-02 15:04:05 MST"
)

var (
	snapshotCreateCommand = snapshotCommands.Command("create", "Creates a snapshot of local directory or file.").Default()

	snapshotCreateSources                 = snapshotCreateCommand.Arg("source", "Files or directories to create snapshot(s) of.").ExistingFilesOrDirs()
	snapshotCreateAll                     = snapshotCreateCommand.Flag("all", "Create snapshots for files or directories previously backed up by this user on this computer").Bool()
	snapshotCreateCheckpointUploadLimitMB = snapshotCreateCommand.Flag("upload-limit-mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int64()
	snapshotCreateCheckpointInterval      = snapshotCreateCommand.Flag("checkpoint-interval", "Frequency for creating periodic checkpoint.").Duration()
	snapshotCreateDescription             = snapshotCreateCommand.Flag("description", "Free-form snapshot description.").String()
	snapshotCreateForceHash               = snapshotCreateCommand.Flag("force-hash", "Force hashing of source files for a given percentage of files [0..100]").Default("0").Int()
	snapshotCreateParallelUploads         = snapshotCreateCommand.Flag("parallel", "Upload N files in parallel").PlaceHolder("N").Default("0").Int()
	snapshotCreateStartTime               = snapshotCreateCommand.Flag("start-time", "Override snapshot start timestamp.").String()
	snapshotCreateEndTime                 = snapshotCreateCommand.Flag("end-time", "Override snapshot end timestamp.").String()
)

func runSnapshotCommand(ctx context.Context, rep repo.Repository) error {
	sources := *snapshotCreateSources

	maybeAutoUpgradeRepository(ctx, rep)

	if *snapshotCreateAll {
		local, err := getLocalBackupPaths(ctx, rep)
		if err != nil {
			return err
		}

		sources = append(sources, local...)
	}

	if len(sources) == 0 {
		return errors.New("no snapshot sources")
	}

	if err := validateStartEndTime(*snapshotCreateStartTime, *snapshotCreateEndTime); err != nil {
		return err
	}

	if len(*snapshotCreateDescription) > maxSnapshotDescriptionLength {
		return errors.New("description too long")
	}

	u := setupUploader(rep)

	var finalErrors []string

	for _, snapshotDir := range sources {
		if u.IsCanceled() {
			printStderr("Upload canceled\n")
			break
		}

		dir, err := filepath.Abs(snapshotDir)
		if err != nil {
			return errors.Errorf("invalid source: '%s': %s", snapshotDir, err)
		}

		sourceInfo := snapshot.SourceInfo{
			Path:     filepath.Clean(dir),
			Host:     rep.Hostname(),
			UserName: rep.Username(),
		}

		if err := snapshotSingleSource(ctx, rep, u, sourceInfo); err != nil {
			finalErrors = append(finalErrors, err.Error())
		}
	}

	if len(finalErrors) == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors:\n%v", len(finalErrors), strings.Join(finalErrors, "\n"))
}

func validateStartEndTime(st, et string) error {
	startTime, err := parseTimestamp(st)
	if err != nil {
		return errors.Wrap(err, "could not parse start-time")
	}

	endTime, err := parseTimestamp(et)
	if err != nil {
		return errors.Wrap(err, "could not parse end-time")
	}

	if startTimeAfterEndTime(startTime, endTime) {
		return errors.New("start time override cannot be after the end time override")
	}

	return nil
}

func setupUploader(rep repo.Repository) *snapshotfs.Uploader {
	u := snapshotfs.NewUploader(rep)
	u.MaxUploadBytes = *snapshotCreateCheckpointUploadLimitMB << 20 //nolint:gomnd

	if interval := *snapshotCreateCheckpointInterval; interval != 0 {
		u.CheckpointInterval = interval
	}

	u.ForceHashPercentage = *snapshotCreateForceHash
	u.ParallelUploads = *snapshotCreateParallelUploads
	onCtrlC(u.Cancel)

	u.Progress = progress

	return u
}

func parseTimestamp(timestamp string) (time.Time, error) {
	if timestamp == "" {
		return time.Time{}, nil
	}

	return time.Parse(timeFormat, timestamp)
}

func startTimeAfterEndTime(startTime, endTime time.Time) bool {
	return !startTime.IsZero() &&
		!endTime.IsZero() &&
		startTime.After(endTime)
}

func snapshotSingleSource(ctx context.Context, rep repo.Repository, u *snapshotfs.Uploader, sourceInfo snapshot.SourceInfo) error {
	printStderr("Snapshotting %v ...\n", sourceInfo)

	t0 := time.Now()

	localEntry, err := getLocalFSEntry(ctx, sourceInfo.Path)
	if err != nil {
		return errors.Wrap(err, "unable to get local filesystem entry")
	}

	previous, err := findPreviousSnapshotManifest(ctx, rep, sourceInfo, nil)
	if err != nil {
		return err
	}

	policyTree, err := policy.TreeForSource(ctx, rep, sourceInfo)
	if err != nil {
		return errors.Wrap(err, "unable to get policy tree")
	}

	log(ctx).Debugf("uploading %v using %v previous manifests", sourceInfo, len(previous))

	manifest, err := u.Upload(ctx, localEntry, policyTree, sourceInfo, previous...)
	if err != nil {
		return err
	}

	manifest.Description = *snapshotCreateDescription
	startTimeOverride, _ := parseTimestamp(*snapshotCreateStartTime)
	endTimeOverride, _ := parseTimestamp(*snapshotCreateEndTime)

	if !startTimeOverride.IsZero() {
		if endTimeOverride.IsZero() {
			// Calculate the correct end time based on current duration if they're not specified
			duration := manifest.EndTime.Sub(manifest.StartTime)
			manifest.EndTime = startTimeOverride.Add(duration)
		}

		manifest.StartTime = startTimeOverride
	}

	if !endTimeOverride.IsZero() {
		if startTimeOverride.IsZero() {
			inverseDuration := manifest.StartTime.Sub(manifest.EndTime)
			manifest.StartTime = endTimeOverride.Add(inverseDuration)
		}

		manifest.EndTime = endTimeOverride
	}

	snapID, err := snapshot.SaveSnapshot(ctx, rep, manifest)
	if err != nil {
		return errors.Wrap(err, "cannot save manifest")
	}

	if _, err = policy.ApplyRetentionPolicy(ctx, rep, sourceInfo, true); err != nil {
		return errors.Wrap(err, "unable to apply retention policy")
	}

	if ferr := rep.Flush(ctx); ferr != nil {
		return errors.Wrap(ferr, "flush error")
	}

	progress.Finish()

	var maybePartial string
	if manifest.IncompleteReason != "" {
		maybePartial = " partial"
	}

	if ds := manifest.RootEntry.DirSummary; ds != nil {
		if ds.NumFailed > 0 {
			errorColor.Fprintf(os.Stderr, "\nIgnored %v errors while snapshotting.", ds.NumFailed) //nolint:errcheck
		}
	}

	printStderr("\nCreated%v snapshot with root %v and ID %v in %v\n", maybePartial, manifest.RootObjectID(), snapID, time.Since(t0).Truncate(time.Second))

	return err
}

// findPreviousSnapshotManifest returns the list of previous snapshots for a given source, including
// last complete snapshot and possibly some number of incomplete snapshots following it.
func findPreviousSnapshotManifest(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, noLaterThan *time.Time) ([]*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous snapshots")
	}

	// phase 1 - find latest complete snapshot.
	var previousComplete *snapshot.Manifest

	var previousCompleteStartTime time.Time

	var result []*snapshot.Manifest

	for _, p := range man {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}

		if p.IncompleteReason == "" && (previousComplete == nil || p.StartTime.After(previousComplete.StartTime)) {
			previousComplete = p
			previousCompleteStartTime = p.StartTime
		}
	}

	if previousComplete != nil {
		result = append(result, previousComplete)
	}

	// add all incomplete snapshots after that
	for _, p := range man {
		if noLaterThan != nil && p.StartTime.After(*noLaterThan) {
			continue
		}

		if p.IncompleteReason != "" && p.StartTime.After(previousCompleteStartTime) {
			result = append(result, p)
		}
	}

	return result, nil
}

func getLocalBackupPaths(ctx context.Context, rep repo.Repository) ([]string, error) {
	log(ctx).Debugf("Looking for previous backups of '%v@%v'...", rep.Hostname(), rep.Username())

	sources, err := snapshot.ListSources(ctx, rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list sources")
	}

	var result []string

	for _, src := range sources {
		if src.Host == rep.Hostname() && src.UserName == rep.Username() {
			result = append(result, src.Path)
		}
	}

	return result, nil
}

func init() {
	snapshotCreateCommand.Action(repositoryAction(runSnapshotCommand))
}
