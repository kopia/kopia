package cli

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	restoreCommandHelp = `Restore a directory or file from a snapshot into the specified target path.

By default, the target path will be created by the restore command if it does
not exist.

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

The restore will only attempt to overwrite an existing file system entry if
it is the same type as in the source. For example a if restoring a symlink,
an existing symlink with the same name will be overwritten, but a directory
with the same name will not; an error will be thrown instead.
`
	restoreCommandSourcePathHelp = `Source directory ID/path in the form of a
directory ID and optionally a sub-directory path. For example,
'kffbb7c28ea6c34d6cbe555d1cf80faa9' or
'kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2'
`

	bitsPerByte = 8
)

type commandRestore struct {
	restoreSourceID               string
	restoreTargetPath             string
	restoreOverwriteDirectories   bool
	restoreOverwriteFiles         bool
	restoreOverwriteSymlinks      bool
	restoreConsistentAttributes   bool
	restoreMode                   string
	restoreParallel               int
	restoreIgnorePermissionErrors bool
	restoreSkipTimes              bool
	restoreSkipOwners             bool
	restoreSkipPermissions        bool
	restoreIncremental            bool
	restoreIgnoreErrors           bool
}

func (c *commandRestore) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("restore", restoreCommandHelp)
	cmd.Arg("source", restoreCommandSourcePathHelp).Required().StringVar(&c.restoreSourceID)
	cmd.Arg("target-path", "Path of the directory for the contents to be restored").Required().StringVar(&c.restoreTargetPath)
	cmd.Flag("overwrite-directories", "Overwrite existing directories").Default("true").BoolVar(&c.restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").Default("true").BoolVar(&c.restoreOverwriteFiles)
	cmd.Flag("overwrite-symlinks", "Specifies whether or not to overwrite already existing symlinks").Default("true").BoolVar(&c.restoreOverwriteSymlinks)
	cmd.Flag("consistent-attributes", "When multiple snapshots match, fail if they have inconsistent attributes").Envar("KOPIA_RESTORE_CONSISTENT_ATTRIBUTES").BoolVar(&c.restoreConsistentAttributes)
	cmd.Flag("mode", "Override restore mode").Default(restoreModeAuto).EnumVar(&c.restoreMode, restoreModeAuto, restoreModeLocal, restoreModeZip, restoreModeZipNoCompress, restoreModeTar, restoreModeTgz)
	cmd.Flag("parallel", "Restore parallelism (1=disable)").Default("8").IntVar(&c.restoreParallel)
	cmd.Flag("skip-owners", "Skip owners during restore").BoolVar(&c.restoreSkipOwners)
	cmd.Flag("skip-permissions", "Skip permissions during restore").BoolVar(&c.restoreSkipPermissions)
	cmd.Flag("skip-times", "Skip times during restore").BoolVar(&c.restoreSkipTimes)
	cmd.Flag("ignore-permission-errors", "Ignore permission errors").Default("true").BoolVar(&c.restoreIgnorePermissionErrors)
	cmd.Flag("ignore-errors", "Ignore all errors").BoolVar(&c.restoreIgnoreErrors)
	cmd.Flag("skip-existing", "Skip files and symlinks that exist in the output").BoolVar(&c.restoreIncremental)
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

func (c *commandRestore) restoreOutput(ctx context.Context) (restore.Output, error) {
	p, err := filepath.Abs(c.restoreTargetPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve path")
	}

	m := c.detectRestoreMode(ctx, c.restoreMode)
	switch m {
	case restoreModeLocal:
		return &restore.FilesystemOutput{
			TargetPath:             p,
			OverwriteDirectories:   c.restoreOverwriteDirectories,
			OverwriteFiles:         c.restoreOverwriteFiles,
			OverwriteSymlinks:      c.restoreOverwriteSymlinks,
			IgnorePermissionErrors: c.restoreIgnorePermissionErrors,
			SkipOwners:             c.restoreSkipOwners,
			SkipPermissions:        c.restoreSkipPermissions,
			SkipTimes:              c.restoreSkipTimes,
		}, nil

	case restoreModeZip, restoreModeZipNoCompress:
		f, err := os.Create(c.restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		method := zip.Deflate
		if m == restoreModeZipNoCompress {
			method = zip.Store
		}

		return restore.NewZipOutput(f, method), nil

	case restoreModeTar:
		f, err := os.Create(c.restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(f), nil

	case restoreModeTgz:
		f, err := os.Create(c.restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(gzip.NewWriter(f)), nil

	default:
		return nil, errors.Errorf("unknown mode %v", m)
	}
}

func (c *commandRestore) detectRestoreMode(ctx context.Context, m string) string {
	if m != "auto" {
		return m
	}

	switch {
	case strings.HasSuffix(c.restoreTargetPath, ".zip"):
		log(ctx).Infof("Restoring to a zip file (%v)...", c.restoreTargetPath)
		return restoreModeZip

	case strings.HasSuffix(c.restoreTargetPath, ".tar"):
		log(ctx).Infof("Restoring to an uncompressed tar file (%v)...", c.restoreTargetPath)
		return restoreModeTar

	case strings.HasSuffix(c.restoreTargetPath, ".tar.gz") || strings.HasSuffix(c.restoreTargetPath, ".tgz"):
		log(ctx).Infof("Restoring to a tar+gzip file (%v)...", c.restoreTargetPath)
		return restoreModeTgz

	default:
		log(ctx).Infof("Restoring to local filesystem (%v) with parallelism=%v...", c.restoreTargetPath, c.restoreParallel)
		return restoreModeLocal
	}
}

func printRestoreStats(ctx context.Context, st restore.Stats) {
	var maybeSkipped, maybeErrors string

	if st.SkippedCount > 0 {
		maybeSkipped = fmt.Sprintf(", skipped %v (%v)", st.SkippedCount, units.BytesStringBase10(st.SkippedTotalFileSize))
	}

	if st.IgnoredErrorCount > 0 {
		maybeErrors = fmt.Sprintf(", ignored %v errors", st.IgnoredErrorCount)
	}

	log(ctx).Infof("Restored %v files, %v directories and %v symbolic links (%v)%v%v.\n",
		st.RestoredFileCount,
		st.RestoredDirCount,
		st.RestoredSymlinkCount,
		units.BytesStringBase10(st.RestoredTotalFileSize),
		maybeSkipped, maybeErrors)
}

func (c *commandRestore) run(ctx context.Context, rep repo.Repository) error {
	output, err := c.restoreOutput(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to initialize output")
	}

	rootEntry, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, c.restoreSourceID, c.restoreConsistentAttributes)
	if err != nil {
		return errors.Wrap(err, "unable to get filesystem entry")
	}

	eta := timetrack.Start()

	st, err := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
		Parallel:     c.restoreParallel,
		Incremental:  c.restoreIncremental,
		IgnoreErrors: c.restoreIgnoreErrors,
		ProgressCallback: func(ctx context.Context, stats restore.Stats) {
			restoredCount := stats.RestoredFileCount + stats.RestoredDirCount + stats.RestoredSymlinkCount + stats.SkippedCount
			enqueuedCount := stats.EnqueuedFileCount + stats.EnqueuedDirCount + stats.EnqueuedSymlinkCount

			if restoredCount == 0 {
				return
			}

			var maybeRemaining, maybeSkipped, maybeErrors string

			if est, ok := eta.Estimate(float64(stats.RestoredTotalFileSize), float64(stats.EnqueuedTotalFileSize)); ok {
				bitsPerSecond := est.SpeedPerSecond * float64(bitsPerByte)
				maybeRemaining = fmt.Sprintf(" %v (%.1f%%) remaining %v",
					units.BitsPerSecondsString(bitsPerSecond),
					est.PercentComplete,
					est.Remaining)
			}

			if stats.SkippedCount > 0 {
				maybeSkipped = fmt.Sprintf(", skipped %v (%v)", stats.SkippedCount, units.BytesStringBase10(stats.SkippedTotalFileSize))
			}

			if stats.IgnoredErrorCount > 0 {
				maybeErrors = fmt.Sprintf(", ignored %v errors", stats.IgnoredErrorCount)
			}

			log(ctx).Infof("Processed %v (%v) of %v (%v)%v%v%v.",
				restoredCount, units.BytesStringBase10(stats.RestoredTotalFileSize),
				enqueuedCount, units.BytesStringBase10(stats.EnqueuedTotalFileSize),
				maybeSkipped,
				maybeErrors,
				maybeRemaining)
		},
	})
	if err != nil {
		return errors.Wrap(err, "error restoring")
	}

	printRestoreStats(ctx, st)

	return nil
}
