package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	maxSnapshotDescriptionLength = 1024
	timeFormat                   = "2006-01-02 15:04:05 MST"
)

type commandSnapshotCreate struct {
	snapshotCreateSources                 []string
	snapshotCreateAll                     bool
	snapshotCreateDescription             string
	snapshotCreateCheckpointInterval      time.Duration
	snapshotCreateFailFast                bool
	snapshotCreateForceHash               float64
	snapshotCreateParallelUploads         int
	snapshotCreateStartTime               string
	snapshotCreateEndTime                 string
	snapshotCreateForceEnableActions      bool
	snapshotCreateForceDisableActions     bool
	snapshotCreateStdinFileName           string
	snapshotCreateCheckpointUploadLimitMB int64
	snapshotCreateTags                    []string
	flushPerSource                        bool

	pins []string

	logDirDetail   int
	logEntryDetail int

	jo  jsonOutput
	svc appServices
	out textOutput
}

func (c *commandSnapshotCreate) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("create", "Creates a snapshot of local directory or file.").Default()

	cmd.Arg("source", "Files or directories to create snapshot(s) of.").StringsVar(&c.snapshotCreateSources)
	cmd.Flag("all", "Create snapshots for files or directories previously backed up by this user on this computer").BoolVar(&c.snapshotCreateAll)
	cmd.Flag("upload-limit-mb", "Stop the backup process after the specified amount of data (in MB) has been uploaded.").PlaceHolder("MB").Default("0").Int64Var(&c.snapshotCreateCheckpointUploadLimitMB)
	cmd.Flag("checkpoint-interval", "Interval between periodic checkpoints (must be <= 45 minutes).").Hidden().DurationVar(&c.snapshotCreateCheckpointInterval)
	cmd.Flag("description", "Free-form snapshot description.").StringVar(&c.snapshotCreateDescription)
	cmd.Flag("fail-fast", "Fail fast when creating snapshot.").Envar(svc.EnvName("KOPIA_SNAPSHOT_FAIL_FAST")).BoolVar(&c.snapshotCreateFailFast)
	cmd.Flag("force-hash", "Force hashing of source files for a given percentage of files [0.0 .. 100.0]").Default("0").Float64Var(&c.snapshotCreateForceHash)
	cmd.Flag("parallel", "Upload N files in parallel").PlaceHolder("N").Default("0").IntVar(&c.snapshotCreateParallelUploads)
	cmd.Flag("start-time", "Override snapshot start timestamp.").StringVar(&c.snapshotCreateStartTime)
	cmd.Flag("end-time", "Override snapshot end timestamp.").StringVar(&c.snapshotCreateEndTime)
	cmd.Flag("force-enable-actions", "Enable snapshot actions even if globally disabled on this client").Hidden().BoolVar(&c.snapshotCreateForceEnableActions)
	cmd.Flag("force-disable-actions", "Disable snapshot actions even if globally enabled on this client").Hidden().BoolVar(&c.snapshotCreateForceDisableActions)
	cmd.Flag("stdin-file", "File path to be used for stdin data snapshot.").StringVar(&c.snapshotCreateStdinFileName)
	cmd.Flag("tags", "Tags applied on the snapshot. Must be provided in the <key>:<value> format.").StringsVar(&c.snapshotCreateTags)
	cmd.Flag("pin", "Create a pinned snapshot that's will not expire automatically").StringsVar(&c.pins)
	cmd.Flag("flush-per-source", "Flush writes at the end of each source").Hidden().BoolVar(&c.flushPerSource)

	c.logDirDetail = -1
	c.logEntryDetail = -1

	cmd.Flag("log-dir-detail", "Override log level for directories").IntVar(&c.logDirDetail)
	cmd.Flag("log-entry-detail", "Override log level for entries").IntVar(&c.logEntryDetail)

	c.jo.setup(svc, cmd)
	c.out.setup(svc)

	c.svc = svc
	cmd.Action(svc.repositoryWriterAction(c.run))
}

//nolint:gocyclo
func (c *commandSnapshotCreate) run(ctx context.Context, rep repo.RepositoryWriter) error {
	sources := c.snapshotCreateSources

	if err := maybeAutoUpgradeRepository(ctx, rep); err != nil {
		return errors.Wrap(err, "error upgrading repository")
	}

	if c.snapshotCreateAll {
		local, err := getLocalBackupPaths(ctx, rep)
		if err != nil {
			return err
		}

		sources = append(sources, local...)
	}

	if len(sources) == 0 {
		return errors.New("no snapshot sources")
	}

	if err := validateStartEndTime(c.snapshotCreateStartTime, c.snapshotCreateEndTime); err != nil {
		return err
	}

	if len(c.snapshotCreateDescription) > maxSnapshotDescriptionLength {
		return errors.New("description too long")
	}

	u := c.setupUploader(rep)

	var finalErrors []string

	tags, err := getTags(c.snapshotCreateTags)
	if err != nil {
		return err
	}

	for _, snapshotDir := range sources {
		if u.IsCanceled() {
			log(ctx).Infof("Upload canceled")
			break
		}

		dir, err := filepath.Abs(snapshotDir)
		if err != nil {
			finalErrors = append(finalErrors, fmt.Sprintf("invalid source: '%s': %s", snapshotDir, err))
			continue
		}

		sourceInfo := snapshot.SourceInfo{
			Path:     filepath.Clean(dir),
			Host:     rep.ClientOptions().Hostname,
			UserName: rep.ClientOptions().Username,
		}

		if err := c.snapshotSingleSource(ctx, rep, u, sourceInfo, tags); err != nil {
			finalErrors = append(finalErrors, err.Error())
		}
	}

	// ensure we flush at least once in the session to properly close all pending buffers,
	// otherwise the session will be reported as memory leak.
	// by default the wrapper function does not flush on errors, which is what we want to do always.
	if !c.flushPerSource {
		if ferr := rep.Flush(ctx); ferr != nil {
			return errors.Wrap(ferr, "flush error")
		}
	}

	if len(finalErrors) == 0 {
		return nil
	}

	if len(finalErrors) == 1 {
		return errors.New(finalErrors[0])
	}

	return errors.Errorf("encountered %v errors:\n%v", len(finalErrors), strings.Join(finalErrors, "\n"))
}

func getTags(tagStrings []string) (map[string]string, error) {
	numberOfPartsInTagString := 2
	// tagKeyPrefix is the prefix for user defined tag keys.
	tagKeyPrefix := "tag:"

	tags := map[string]string{}

	for _, tagkv := range tagStrings {
		parts := strings.SplitN(tagkv, ":", numberOfPartsInTagString)
		if len(parts) != numberOfPartsInTagString {
			return nil, errors.Errorf("Invalid tag format (%s). Requires <key>:<value>", tagkv)
		}

		key := tagKeyPrefix + parts[0]
		if _, ok := tags[key]; ok {
			return nil, errors.Errorf("Duplicate tag <key> found. (%s)", parts[0])
		}

		tags[key] = parts[1]
	}

	return tags, nil
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

func (c *commandSnapshotCreate) setupUploader(rep repo.RepositoryWriter) *snapshotfs.Uploader {
	u := snapshotfs.NewUploader(rep)
	u.MaxUploadBytes = c.snapshotCreateCheckpointUploadLimitMB << 20 //nolint:gomnd

	if c.snapshotCreateForceEnableActions {
		u.EnableActions = true
	}

	if c.snapshotCreateForceDisableActions {
		u.EnableActions = false
	}

	if l := c.logDirDetail; l != -1 {
		ld := policy.LogDetail(l)

		u.OverrideDirLogDetail = &ld
	}

	if l := c.logEntryDetail; l != -1 {
		ld := policy.LogDetail(l)

		u.OverrideEntryLogDetail = &ld
	}

	if interval := c.snapshotCreateCheckpointInterval; interval != 0 {
		u.CheckpointInterval = interval
	}

	c.svc.onCtrlC(u.Cancel)

	u.ForceHashPercentage = c.snapshotCreateForceHash
	u.ParallelUploads = c.snapshotCreateParallelUploads

	u.FailFast = c.snapshotCreateFailFast
	u.Progress = c.svc.getProgress()

	return u
}

func parseTimestamp(timestamp string) (time.Time, error) {
	if timestamp == "" {
		return time.Time{}, nil
	}

	//nolint:wrapcheck
	return time.Parse(timeFormat, timestamp)
}

func startTimeAfterEndTime(startTime, endTime time.Time) bool {
	return !startTime.IsZero() &&
		!endTime.IsZero() &&
		startTime.After(endTime)
}

//nolint:gocyclo
func (c *commandSnapshotCreate) snapshotSingleSource(ctx context.Context, rep repo.RepositoryWriter, u *snapshotfs.Uploader, sourceInfo snapshot.SourceInfo, tags map[string]string) error {
	log(ctx).Infof("Snapshotting %v ...", sourceInfo)

	var (
		err       error
		fsEntry   fs.Entry
		setManual bool
	)

	if c.snapshotCreateStdinFileName != "" {
		// stdin source will be snapshotted using a virtual static root directory with a single streaming file entry
		// Create a new static directory with the given name and add a streaming file entry with os.Stdin reader
		fsEntry = virtualfs.NewStaticDirectory(sourceInfo.Path, []fs.Entry{
			virtualfs.StreamingFileFromReader(c.snapshotCreateStdinFileName, c.svc.stdin()),
		})
		setManual = true
	} else {
		fsEntry, err = getLocalFSEntry(ctx, sourceInfo.Path)
		if err != nil {
			return errors.Wrap(err, "unable to get local filesystem entry")
		}
	}

	previous, err := findPreviousSnapshotManifest(ctx, rep, sourceInfo, nil)
	if err != nil {
		return err
	}

	policyTree, err := policy.TreeForSource(ctx, rep, sourceInfo)
	if err != nil {
		return errors.Wrap(err, "unable to get policy tree")
	}

	manifest, err := u.Upload(ctx, fsEntry, policyTree, sourceInfo, previous...)
	if err != nil {
		// fail-fast uploads will fail here without recording a manifest, other uploads will
		// possibly fail later.
		return errors.Wrap(err, "upload error")
	}

	manifest.Description = c.snapshotCreateDescription
	manifest.Tags = tags
	manifest.UpdatePins(c.pins, nil)

	startTimeOverride, _ := parseTimestamp(c.snapshotCreateStartTime)
	endTimeOverride, _ := parseTimestamp(c.snapshotCreateEndTime)

	if !startTimeOverride.IsZero() {
		if endTimeOverride.IsZero() {
			// Calculate the correct end time based on current duration if they're not specified
			duration := manifest.EndTime.Sub(manifest.StartTime)
			manifest.EndTime = fs.UTCTimestampFromTime(startTimeOverride).Add(duration)
		}

		manifest.StartTime = fs.UTCTimestampFromTime(startTimeOverride)
	}

	if !endTimeOverride.IsZero() {
		if startTimeOverride.IsZero() {
			inverseDuration := manifest.StartTime.Sub(manifest.EndTime)
			manifest.StartTime = fs.UTCTimestampFromTime(endTimeOverride).Add(inverseDuration)
		}

		manifest.EndTime = fs.UTCTimestampFromTime(endTimeOverride)
	}

	ignoreIdenticalSnapshot := policyTree.EffectivePolicy().RetentionPolicy.IgnoreIdenticalSnapshots.OrDefault(false)
	if ignoreIdenticalSnapshot && len(previous) > 0 {
		if previous[0].RootObjectID() == manifest.RootObjectID() {
			log(ctx).Infof("\n Not saving snapshot because no files have been changed since previous snapshot")
			return nil
		}
	}

	if _, err = snapshot.SaveSnapshot(ctx, rep, manifest); err != nil {
		return errors.Wrap(err, "cannot save manifest")
	}

	if _, err = policy.ApplyRetentionPolicy(ctx, rep, sourceInfo, true); err != nil {
		return errors.Wrap(err, "unable to apply retention policy")
	}

	if setManual {
		if err = policy.SetManual(ctx, rep, sourceInfo); err != nil {
			return errors.Wrap(err, "unable to set manual field in scheduling policy for source")
		}
	}

	if c.flushPerSource {
		if ferr := rep.Flush(ctx); ferr != nil {
			return errors.Wrap(ferr, "flush error")
		}
	}

	c.svc.getProgress().Finish()

	return c.reportSnapshotStatus(ctx, manifest)
}

func (c *commandSnapshotCreate) reportSnapshotStatus(ctx context.Context, manifest *snapshot.Manifest) error {
	var maybePartial string
	if manifest.IncompleteReason != "" {
		maybePartial = " partial"
	}

	sourceInfo := manifest.Source

	snapID := manifest.ID

	if c.jo.jsonOutput {
		c.out.printStdout("%s\n", c.jo.jsonIndentedBytes(manifest, "  "))
		return nil
	}

	log(ctx).Infof("Created%v snapshot with root %v and ID %v in %v", maybePartial, manifest.RootObjectID(), snapID, manifest.EndTime.Sub(manifest.StartTime).Truncate(time.Second))

	if ds := manifest.RootEntry.DirSummary; ds != nil {
		if ds.IgnoredErrorCount > 0 {
			log(ctx).Warnf("Ignored %v error(s) while snapshotting %v.", ds.IgnoredErrorCount, sourceInfo)
		}

		if ds.FatalErrorCount > 0 {
			return errors.Errorf("Found %v fatal error(s) while snapshotting %v.", ds.FatalErrorCount, sourceInfo) //nolint:revive
		}
	}

	return nil
}

// findPreviousSnapshotManifest returns the list of previous snapshots for a given source, including
// last complete snapshot and possibly some number of incomplete snapshots following it.
func findPreviousSnapshotManifest(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo, noLaterThan *fs.UTCTimestamp) ([]*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous snapshots")
	}

	// phase 1 - find latest complete snapshot.
	var previousComplete *snapshot.Manifest

	var previousCompleteStartTime fs.UTCTimestamp

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
	log(ctx).Debugf("Looking for previous backups of '%v@%v'...", rep.ClientOptions().Hostname, rep.ClientOptions().Username)

	sources, err := snapshot.ListSources(ctx, rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list sources")
	}

	var result []string

	for _, src := range sources {
		// add all sources belonging to the repository user@host
		// ignore sources that have Manual field set to true in the SchedulingPolicy
		includeSource, err := shouldSnapshotSource(ctx, src, rep)
		if err != nil {
			return nil, err
		}

		if includeSource {
			result = append(result, src.Path)
		}
	}

	return result, nil
}

func shouldSnapshotSource(ctx context.Context, src snapshot.SourceInfo, rep repo.Repository) (bool, error) {
	policyTree, err := policy.TreeForSource(ctx, rep, src)
	if err != nil {
		return false, errors.Wrapf(err, "unable to get policy tree for source %v", src)
	}

	return src.Host == rep.ClientOptions().Hostname &&
		src.UserName == rep.ClientOptions().Username &&
		!policy.IsManualSnapshot(policyTree), nil
}
