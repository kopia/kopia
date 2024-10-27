package cli

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	restoreCommandHelp = `Restore a directory or a file.

Restore can operate in two modes:

* from a snapshot: restoring (possibly shallowly) a specified file or
directory from a snapshot into a target path. By default, the target
path will be created by the restore command if it does not exist.

* by expanding a shallow placeholder in situ where the placeholder was
created by a previous restore.

In the from-snapshot mode:

The source to be restored is specified in the form of a directory or file ID and
optionally a sub-directory path.

For example, the following source and target arguments will restore the contents
of the 'kffbb7c28ea6c34d6cbe555d1cf80faa9' directory into a new, local directory
named 'd1'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9 d1'

Similarly, the following command will restore the contents of a subdirectory
'subdir/subdir2' under 'kffbb7c28ea6c34d6cbe555d1cf80faa9'  into a new, local
directory named 'sd2'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2 sd2'

When restoring to a target path that already has existing data, by default
the restore will attempt to overwrite, unless one or more of the following flags
has been set (to prevent overwrite of each type):

--no-overwrite-files
--no-overwrite-directories
--no-overwrite-symlinks

If the '--shallow' option is provided, files and directories this
depth and below in the directory hierarchy will be represented by
compact placeholder files of the form 'entry.kopia-entry' instead of
being restored. (I.e. setting '--shallow' to 0 will only shallow
restore.) Snapshots created of directory contents represented by
placeholder files will be identical to snapshots of the equivalent
fully expanded tree.

In the expanding-a-placeholder mode:

The source to be restored is a pre-existing placeholder entry of the form
'entry.kopia-entry'. The target will be 'entry'. '--shallow' controls the depth
of the expansion and defaults to 0. For example:

'restore d3.kopiadir'

will remove the d3.kopiadir placeholder and restore the referenced repository
contents into path d3 where the contents of the newly created path d3 will
themselves be placeholder files.
`
	restoreCommandSourcePathHelp = `Two forms: 1. Source directory ID/path in the form of a
directory ID and optionally a sub-directory path. For example,
'kffbb7c28ea6c34d6cbe555d1cf80faa9' or
'kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2'
followed by the path of the directory for the contents to be restored.

2. one or more placeholder files of the form path.kopia-entry
`

	unlimitedDepth = math.MaxInt32
)

// RestoreProgress is invoked to report progress during a restore.
type RestoreProgress interface {
	SetCounters(s restore.Stats)
	Flush()
}

type restoreSourceTarget struct {
	source        string
	target        string
	isplaceholder bool
}

type commandRestore struct {
	restoreTargetPaths            []string
	restoreOverwriteDirectories   bool
	restoreOverwriteFiles         bool
	restoreOverwriteSymlinks      bool
	restoreWriteSparseFiles       bool
	restoreConsistentAttributes   bool
	restoreMode                   string
	restoreParallel               int
	restoreIgnorePermissionErrors bool
	restoreWriteFilesAtomically   bool
	restoreSkipTimes              bool
	restoreSkipOwners             bool
	restoreSkipPermissions        bool
	restoreIncremental            bool
	restoreIgnoreErrors           bool
	restoreShallowAtDepth         int32
	minSizeForPlaceholder         int32
	snapshotTime                  string

	restores []restoreSourceTarget

	svc appServices
}

func (c *commandRestore) setup(svc appServices, parent commandParent) {
	c.restoreShallowAtDepth = unlimitedDepth
	c.svc = svc

	cmd := parent.Command("restore", restoreCommandHelp)
	cmd.Arg("sources", restoreCommandSourcePathHelp).Required().StringsVar(&c.restoreTargetPaths)
	cmd.Flag("overwrite-directories", "Overwrite existing directories").Default("true").BoolVar(&c.restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").Default("true").BoolVar(&c.restoreOverwriteFiles)
	cmd.Flag("overwrite-symlinks", "Specifies whether or not to overwrite already existing symlinks").Default("true").BoolVar(&c.restoreOverwriteSymlinks)
	cmd.Flag("write-sparse-files", "When doing a restore, attempt to write files sparsely-allocating the minimum amount of disk space needed.").Default("false").BoolVar(&c.restoreWriteSparseFiles)
	cmd.Flag("consistent-attributes", "When multiple snapshots match, fail if they have inconsistent attributes").Envar(svc.EnvName("KOPIA_RESTORE_CONSISTENT_ATTRIBUTES")).BoolVar(&c.restoreConsistentAttributes)
	cmd.Flag("mode", "Override restore mode").Default(restoreModeAuto).EnumVar(&c.restoreMode, restoreModeAuto, restoreModeLocal, restoreModeZip, restoreModeZipNoCompress, restoreModeTar, restoreModeTgz)
	cmd.Flag("parallel", "Restore parallelism (1=disable)").Default("8").IntVar(&c.restoreParallel)
	cmd.Flag("skip-owners", "Skip owners during restore").BoolVar(&c.restoreSkipOwners)
	cmd.Flag("skip-permissions", "Skip permissions during restore").BoolVar(&c.restoreSkipPermissions)
	cmd.Flag("skip-times", "Skip times during restore").BoolVar(&c.restoreSkipTimes)
	cmd.Flag("ignore-permission-errors", "Ignore permission errors").Default("true").BoolVar(&c.restoreIgnorePermissionErrors)
	cmd.Flag("write-files-atomically", "Write files atomically to disk, ensuring they are either fully committed, or not written at all, preventing partially written files").Default("false").BoolVar(&c.restoreWriteFilesAtomically)
	cmd.Flag("ignore-errors", "Ignore all errors").BoolVar(&c.restoreIgnoreErrors)
	cmd.Flag("skip-existing", "Skip files and symlinks that exist in the output").BoolVar(&c.restoreIncremental)
	cmd.Flag("shallow", "Shallow restore the directory hierarchy starting at this level (default is to deep restore the entire hierarchy.)").Int32Var(&c.restoreShallowAtDepth)
	cmd.Flag("shallow-minsize", "When doing a shallow restore, write actual files instead of placeholders smaller than this size.").Int32Var(&c.minSizeForPlaceholder)
	cmd.Flag("snapshot-time", "When using a path as the source, use the latest snapshot available before this date. Default is latest").Default("latest").StringVar(&c.snapshotTime)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

const (
	restoreModeLocal         = "local"
	restoreModeAuto          = "auto"
	restoreModeZip           = "zip"
	restoreModeZipNoCompress = "zip-nocompress"
	restoreModeTar           = "tar"
	restoreModeTgz           = "tgz"
)

// constructTargetPairs builds the sourceIdPathPairs array for this
// command for the two forms of command: expansion of one or more
// placeholders or restoring of a single source to a single destination.
func (c *commandRestore) constructTargetPairs(rep repo.Repository) error {
	targetPairs := make([]restoreSourceTarget, 0, len(c.restoreTargetPaths))

	for _, p := range c.restoreTargetPaths {
		tp := restore.PathIfPlaceholder(p)
		if tp != "" {
			absp, err := filepath.Abs(p)
			if err != nil {
				return errors.Wrapf(err, "restore can't resolve path for %q", p)
			}

			targetPairs = append(targetPairs, restoreSourceTarget{
				source:        absp,
				target:        restore.PathIfPlaceholder(absp),
				isplaceholder: true,
			})
		}
	}

	switch tplen, restpslen := len(targetPairs), len(c.restoreTargetPaths); {
	case tplen == 0 && restpslen == 1:
		// This means that none of the restoreTargetPaths are placeholders and we
		// have 1 arg: a source path that should also be used as a destination.
		source := c.restoreTargetPaths[0]

		si, err := snapshot.ParseSourceInfo(source, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return errors.Errorf("invalid path to be used as source: '%s': %s", source, err)
		}

		if si.Path == "" {
			return errors.New("the source must contain a path element")
		}

		if si.Host != rep.ClientOptions().Hostname || si.UserName != rep.ClientOptions().Username {
			return errors.New("the source must be a path in with the same username/hostname to be used as a target too")
		}

		c.restores = []restoreSourceTarget{
			{
				source:        source,
				target:        si.Path,
				isplaceholder: false,
			},
		}

		return nil
	case tplen == 0 && restpslen == 2:
		// This means that none of the restoreTargetPaths are placeholders and we
		// have two args: a sourceID and a destination directory.
		absp, err := filepath.Abs(c.restoreTargetPaths[1])
		if err != nil {
			return errors.Wrapf(err, "restore can't resolve path for %q", c.restoreTargetPaths[1])
		}

		c.restores = []restoreSourceTarget{
			{
				source:        c.restoreTargetPaths[0],
				target:        absp,
				isplaceholder: false,
			},
		}

		return nil
	case tplen == restpslen:
		// All arguments are placeholders.
		c.restores = targetPairs
		return nil
	}

	// Some undefined mixture of placeholders and other arguments.
	return errors.New("restore requires a source and targetpath or placeholders")
}

func (c *commandRestore) restoreOutput(ctx context.Context, rep repo.Repository) (restore.Output, error) {
	err := c.constructTargetPairs(rep)
	if err != nil {
		return nil, err
	}

	targetpath := c.restores[0].target

	m := c.detectRestoreMode(ctx, c.restoreMode, targetpath)
	switch m {
	case restoreModeLocal:
		o := &restore.FilesystemOutput{
			TargetPath:             targetpath,
			OverwriteDirectories:   c.restoreOverwriteDirectories,
			OverwriteFiles:         c.restoreOverwriteFiles,
			OverwriteSymlinks:      c.restoreOverwriteSymlinks,
			IgnorePermissionErrors: c.restoreIgnorePermissionErrors,
			WriteFilesAtomically:   c.restoreWriteFilesAtomically,
			SkipOwners:             c.restoreSkipOwners,
			SkipPermissions:        c.restoreSkipPermissions,
			SkipTimes:              c.restoreSkipTimes,
			WriteSparseFiles:       c.restoreWriteSparseFiles,
		}

		if err := o.Init(ctx); err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return o, nil

	case restoreModeZip, restoreModeZipNoCompress:
		f, err := os.Create(targetpath) //nolint:gosec
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		method := zip.Deflate
		if m == restoreModeZipNoCompress {
			method = zip.Store
		}

		return restore.NewZipOutput(f, method), nil

	case restoreModeTar:
		f, err := os.Create(targetpath) //nolint:gosec
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(f), nil

	case restoreModeTgz:
		f, err := os.Create(targetpath) //nolint:gosec
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(gzip.NewWriter(f)), nil

	default:
		return nil, errors.Errorf("unknown mode %v", m)
	}
}

func (c *commandRestore) detectRestoreMode(ctx context.Context, m, targetpath string) string {
	if m != "auto" {
		return m
	}

	switch {
	case strings.HasSuffix(targetpath, ".zip"):
		log(ctx).Infof("Restoring to a zip file (%v)...", targetpath)
		return restoreModeZip

	case strings.HasSuffix(targetpath, ".tar"):
		log(ctx).Infof("Restoring to an uncompressed tar file (%v)...", targetpath)
		return restoreModeTar

	case strings.HasSuffix(targetpath, ".tar.gz") || strings.HasSuffix(targetpath, ".tgz"):
		log(ctx).Infof("Restoring to a tar+gzip file (%v)...", targetpath)
		return restoreModeTgz

	default:
		log(ctx).Infof("Restoring to local filesystem (%v) with parallelism=%v...", targetpath, c.restoreParallel)
		return restoreModeLocal
	}
}

func printRestoreStats(ctx context.Context, st *restore.Stats) {
	var maybeSkipped, maybeErrors string

	if st.SkippedCount > 0 {
		maybeSkipped = fmt.Sprintf(", skipped %v (%v)", st.SkippedCount, units.BytesString(st.SkippedTotalFileSize))
	}

	if st.IgnoredErrorCount > 0 {
		maybeErrors = fmt.Sprintf(", ignored %v errors", st.IgnoredErrorCount)
	}

	log(ctx).Infof("Restored %v files, %v directories and %v symbolic links (%v)%v%v.\n",
		st.RestoredFileCount,
		st.RestoredDirCount,
		st.RestoredSymlinkCount,
		units.BytesString(st.RestoredTotalFileSize),
		maybeSkipped, maybeErrors)
}

func (c *commandRestore) setupPlaceholderExpansion(ctx context.Context, rep repo.Repository, rstp restoreSourceTarget, output restore.Output) (fs.Entry, error) {
	rootEntry, err := snapshotfs.GetEntryFromPlaceholder(ctx, rep, localfs.PlaceholderFilePath(rstp.source))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get filesystem entry for placeholder %q", rstp.source)
	}

	fso, ok := output.(*restore.FilesystemOutput)
	if !ok {
		return nil, errors.New("placeholder expansion is only relevant to filesystem output")
	}

	fso.TargetPath = rstp.target

	// restoreShallowAtDepth defaults to 0 when expanding a placeholder.
	if c.restoreShallowAtDepth == unlimitedDepth {
		c.restoreShallowAtDepth = 0
	}

	return rootEntry, nil
}

func (c *commandRestore) getRestoreProgress() RestoreProgress {
	if rp := c.svc.getRestoreProgress(); rp != nil {
		return rp
	}

	pf := c.svc.getProgress().progressFlags

	return &cliRestoreProgress{
		enableProgress:         pf.enableProgress,
		out:                    pf.out,
		progressUpdateInterval: pf.progressUpdateInterval,
		eta:                    timetrack.Start(),
	}
}

func (c *commandRestore) run(ctx context.Context, rep repo.Repository) error {
	output, oerr := c.restoreOutput(ctx, rep)
	if oerr != nil {
		return errors.Wrap(oerr, "unable to initialize output")
	}

	for _, rstp := range c.restores {
		var rootEntry fs.Entry

		if rstp.isplaceholder {
			re, err := c.setupPlaceholderExpansion(ctx, rep, rstp, output)
			if err != nil {
				return errors.Wrap(err, "placeholder can't be reified")
			}

			rootEntry = re
		} else {
			source, err := c.tryToConvertPathToID(ctx, rep, rstp.source)
			if err != nil {
				return err
			}

			re, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, source, c.restoreConsistentAttributes)
			if err != nil {
				return errors.Wrap(err, "unable to get filesystem entry")
			}

			rootEntry = re
		}

		restoreProgress := c.getRestoreProgress()
		progressCallback := func(ctx context.Context, stats restore.Stats) {
			restoreProgress.SetCounters(stats)
		}

		st, err := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
			Parallel:               c.restoreParallel,
			Incremental:            c.restoreIncremental,
			IgnoreErrors:           c.restoreIgnoreErrors,
			RestoreDirEntryAtDepth: c.restoreShallowAtDepth,
			MinSizeForPlaceholder:  c.minSizeForPlaceholder,
			ProgressCallback:       progressCallback,
		})
		if err != nil {
			return errors.Wrap(err, "error restoring")
		}

		progressCallback(ctx, st)
		restoreProgress.Flush() // Force last progress values to be printed
		printRestoreStats(ctx, &st)
	}

	return nil
}

// tryToConvertPathToID checks if the source is a path and in this case returns the ID of the snapshot
// containing the latest version available.
func (c *commandRestore) tryToConvertPathToID(ctx context.Context, rep repo.Repository, source string) (string, error) {
	pathElements := strings.Split(filepath.ToSlash(source), "/")

	if pathElements[0] != "" {
		_, err := object.ParseID(pathElements[0])
		if err == nil {
			// source is an ID
			return source, nil
		}
	}

	// Consider source as a path

	if c.snapshotTime == "" {
		return "", errors.New("a snapshot time is needed to use a path as source")
	}

	filter, err := createSnapshotTimeFilter(c.snapshotTime)
	if err != nil {
		return "", err
	}

	si, err := snapshot.ParseSourceInfo(source, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
	if err != nil {
		return "", errors.Errorf("invalid directory: '%s': %s", source, err)
	}

	if si.Path == "" {
		return "", errors.New("the source must contain a path element")
	}

	manifestIDs, err := findSnapshotsForSource(ctx, rep, si, map[string]string{})
	if err != nil {
		return "", err
	}

	if len(manifestIDs) == 0 {
		return "", errors.Errorf("no snapshots contain data for %v", source)
	}

	ms, err := snapshot.LoadSnapshots(ctx, rep, manifestIDs)
	if err != nil {
		return "", errors.Wrap(err, "unable to load snapshots")
	}

	m, relPath, ohid := findLastManifestWithPath(ctx, rep, ms, si.Path, filter)
	if m == nil {
		return "", errors.Errorf("no snapshots contain data for %v", source)
	}

	log(ctx).Infof("Restoring from\n"+
		"   Snapshot source: %v\n"+
		"   Snapshot time: %v\n"+
		"   Relative path: %v\n"+
		"   Object ID: %v", m.Source, formatTimestamp(m.StartTime.ToTime()), relPath, ohid)

	return ohid.String(), nil
}

func createSnapshotTimeFilter(timespec string) (func(*snapshot.Manifest, int, int) bool, error) {
	if timespec == "" || timespec == "latest" {
		return func(_ *snapshot.Manifest, i, _ int) bool {
			return i == 0
		}, nil
	}

	if timespec == "oldest" {
		return func(_ *snapshot.Manifest, i, total int) bool {
			return i == total-1
		}, nil
	}

	t, err := computeMaxTime(timespec)
	if err != nil {
		return nil, err
	}

	return func(m *snapshot.Manifest, _, _ int) bool {
		return m.StartTime.ToTime().Before(t)
	}, nil
}

var timeAgoRE = regexp.MustCompile(`^(?:(\d{1,3})(?:y|year|years)-)?(?:(\d{1,3})(?:m|mo|month|months)-)?(?:(\d{1,3})(?:d|day|days)-)?ago$`)

// computeMaxTime returns the first time after the max allowed.
func computeMaxTime(timespec string) (time.Time, error) {
	now := clock.Now()

	if timespec == "yesterday" {
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local), nil
	}

	if timespec == "last-month" {
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.Local), nil
	}

	if timespec == "last-year" {
		return time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.Local), nil
	}

	if strings.HasSuffix(timespec, "-ago") {
		ymd := timeAgoRE.FindStringSubmatch(timespec)
		if ymd != nil {
			t := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)

			years, _ := strconv.Atoi(ymd[1])
			months, _ := strconv.Atoi(ymd[2])
			days, _ := strconv.Atoi(ymd[3])

			// +1 to compute end time of current day
			return t.AddDate(-years, -months, -days+1), nil
		}
	}

	// Just used as markers, the value does not really matter
	day := 24 * time.Hour //nolint:mnd
	month := 30 * day     //nolint:mnd
	year := 12 * month    //nolint:mnd

	formats := []struct {
		format    string
		precision time.Duration
	}{
		// Used by kopia output
		{"2006-01-02 15:04:05 MST", time.Second},
		{"2006-01-02 15:04:05.000 MST", time.Millisecond},

		// Others
		{"2006-1-2T15:04:05Z07:00", time.Second},
		{"2006-1-2T15:04:05Z0700", time.Second},
		{"2006-1-2T15:04:05Z07", time.Second},
		{"2006-1-2T15:04:05", time.Second},
		{"2006-1-2T15:04Z07", time.Minute},
		{"2006-1-2T15:04", time.Minute},
		{"2006-1-2T15", time.Hour},
		{"2006-1-2 15:04:05Z0700", time.Second},
		{"2006-1-2 15:04:05Z07", time.Second},
		{"2006-1-2 15:04:05", time.Second},
		{"2006-1-2 15:04Z07", time.Minute},
		{"2006-1-2 15:04", time.Minute},
		{"2006-1-2 15", time.Hour},
		{"2006-1-2", day},
		{"2006-1", month},
		{"2006", year},
	}
	for _, f := range formats {
		t, err := time.Parse(f.format, timespec)
		if err != nil {
			continue
		}

		// If no timezone is given, assume local time
		if !strings.Contains(f.format, "Z") && !strings.Contains(f.format, "MST") {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.Local)
		}

		switch f.precision {
		case year:
			t = t.AddDate(1, 0, 0)
		case month:
			t = t.AddDate(0, 1, 0)
		case day:
			t = t.AddDate(0, 0, 1)
		default:
			t = t.Add(f.precision)
		}

		return t, nil
	}

	return now, errors.Errorf("Invalid time spec: %v", timespec)
}

func findLastManifestWithPath(ctx context.Context, rep repo.Repository, ms []*snapshot.Manifest, path string, filter func(*snapshot.Manifest, int, int) bool) (*snapshot.Manifest, string, object.ID) {
	ms = snapshot.SortByTime(ms, true)

	type candidateInfo struct {
		m    *snapshot.Manifest
		pe   []string
		ohid object.HasObjectID
	}

	var candidates []candidateInfo

	for _, m := range ms {
		if m.IncompleteReason != "" {
			// Ignore this snapshot
			continue
		}

		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			// Ignore this snapshot
			continue
		}

		pathElements, err := findRelativePathParts(m, path)
		if err != nil {
			// Ignore this snapshot
			continue
		}

		ent, err := snapshotfs.GetNestedEntry(ctx, root, pathElements)
		if err != nil {
			// Ignore this snapshot
			continue
		}

		ohid, ok := ent.(object.HasObjectID)
		if !ok {
			// Ignore this snapshot
			continue
		}

		candidates = append(candidates, candidateInfo{m, pathElements, ohid})
	}

	for i, c := range candidates {
		if !filter(c.m, i, len(candidates)) {
			// Ignore this snapshot
			continue
		}

		return c.m, filepath.Join(c.pe...), c.ohid.ObjectID()
	}

	return nil, "", object.ID{}
}
