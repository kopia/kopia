package cli

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
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

var (
	restoreCommand                = app.Command("restore", restoreCommandHelp)
	restoreSourceID               = ""
	restoreTargetPath             = ""
	restoreOverwriteDirectories   = true
	restoreOverwriteFiles         = true
	restoreOverwriteSymlinks      = true
	restoreConsistentAttributes   = false
	restoreMode                   = restoreModeAuto
	restoreParallel               = 8
	restoreIgnorePermissionErrors = true
	restoreSkipTimes              = false
	restoreSkipOwners             = false
	restoreSkipPermissions        = false
	restoreIncremental            = false
	restoreIgnoreErrors           = false
)

const (
	restoreModeLocal         = "local"
	restoreModeAuto          = "auto"
	restoreModeZip           = "zip"
	restoreModeZipNoCompress = "zip-nocompress"
	restoreModeTar           = "tar"
	restoreModeTgz           = "tgz"
)

func addRestoreFlags(cmd *kingpin.CmdClause) {
	cmd.Arg("source", restoreCommandSourcePathHelp).Required().StringVar(&restoreSourceID)
	cmd.Arg("target-path", "Path of the directory for the contents to be restored").Required().StringVar(&restoreTargetPath)
	cmd.Flag("overwrite-directories", "Overwrite existing directories").BoolVar(&restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").BoolVar(&restoreOverwriteFiles)
	cmd.Flag("overwrite-symlinks", "Specifies whether or not to overwrite already existing symlinks").BoolVar(&restoreOverwriteSymlinks)
	cmd.Flag("consistent-attributes", "When multiple snapshots match, fail if they have inconsistent attributes").Envar("KOPIA_RESTORE_CONSISTENT_ATTRIBUTES").BoolVar(&restoreConsistentAttributes)
	cmd.Flag("mode", "Override restore mode").EnumVar(&restoreMode, restoreModeAuto, restoreModeLocal, restoreModeZip, restoreModeZipNoCompress, restoreModeTar, restoreModeTgz)
	cmd.Flag("parallel", "Restore parallelism (1=disable)").IntVar(&restoreParallel)
	cmd.Flag("skip-owners", "Skip owners during restore").BoolVar(&restoreSkipOwners)
	cmd.Flag("skip-permissions", "Skip permissions during restore").BoolVar(&restoreSkipPermissions)
	cmd.Flag("skip-times", "Skip times during restore").BoolVar(&restoreSkipTimes)
	cmd.Flag("ignore-permission-errors", "Ignore permission errors").BoolVar(&restoreIgnorePermissionErrors)
	cmd.Flag("ignore-errors", "Ignore all errors").BoolVar(&restoreIgnoreErrors)
	cmd.Flag("skip-existing", "Skip files and symlinks that exist in the output").BoolVar(&restoreIncremental)
}

func restoreOutput(ctx context.Context) (restore.Output, error) {
	p, err := filepath.Abs(restoreTargetPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve path")
	}

	m := detectRestoreMode(ctx, restoreMode)
	switch m {
	case restoreModeLocal:
		return &restore.FilesystemOutput{
			TargetPath:             p,
			OverwriteDirectories:   restoreOverwriteDirectories,
			OverwriteFiles:         restoreOverwriteFiles,
			OverwriteSymlinks:      restoreOverwriteSymlinks,
			IgnorePermissionErrors: restoreIgnorePermissionErrors,
			SkipOwners:             restoreSkipOwners,
			SkipPermissions:        restoreSkipPermissions,
			SkipTimes:              restoreSkipTimes,
		}, nil

	case restoreModeZip, restoreModeZipNoCompress:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		method := zip.Deflate
		if m == restoreModeZipNoCompress {
			method = zip.Store
		}

		return restore.NewZipOutput(f, method), nil

	case restoreModeTar:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(f), nil

	case restoreModeTgz:
		f, err := os.Create(restoreTargetPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to create output file")
		}

		return restore.NewTarOutput(gzip.NewWriter(f)), nil

	default:
		return nil, errors.Errorf("unknown mode %v", m)
	}
}

func detectRestoreMode(ctx context.Context, m string) string {
	if m != "auto" {
		return m
	}

	switch {
	case strings.HasSuffix(restoreTargetPath, ".zip"):
		log(ctx).Infof("Restoring to a zip file (%v)...", restoreTargetPath)
		return restoreModeZip

	case strings.HasSuffix(restoreTargetPath, ".tar"):
		log(ctx).Infof("Restoring to an uncompressed tar file (%v)...", restoreTargetPath)
		return restoreModeTar

	case strings.HasSuffix(restoreTargetPath, ".tar.gz") || strings.HasSuffix(restoreTargetPath, ".tgz"):
		log(ctx).Infof("Restoring to a tar+gzip file (%v)...", restoreTargetPath)
		return restoreModeTgz

	default:
		log(ctx).Infof("Restoring to local filesystem (%v) with parallelism=%v...", restoreTargetPath, restoreParallel)
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

func runRestoreCommand(ctx context.Context, rep repo.Repository) error {
	output, err := restoreOutput(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to initialize output")
	}

	rootEntry, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, restoreSourceID, restoreConsistentAttributes)
	if err != nil {
		return errors.Wrap(err, "unable to get filesystem entry")
	}

	t0 := clock.Now()

	st, err := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
		Parallel:     restoreParallel,
		Incremental:  restoreIncremental,
		IgnoreErrors: restoreIgnoreErrors,
		ProgressCallback: func(ctx context.Context, stats restore.Stats) {
			restoredCount := stats.RestoredFileCount + stats.RestoredDirCount + stats.RestoredSymlinkCount + stats.SkippedCount
			enqueuedCount := stats.EnqueuedFileCount + stats.EnqueuedDirCount + stats.EnqueuedSymlinkCount

			if restoredCount == 0 {
				return
			}

			var maybeRemaining, maybeSkipped, maybeErrors string

			if stats.EnqueuedTotalFileSize > 0 {
				progress := float64(stats.RestoredTotalFileSize) / float64(stats.EnqueuedTotalFileSize)
				elapsed := clock.Since(t0)
				if progress > 0 && elapsed.Seconds() > 1 {
					predictedDuration := time.Duration(1e9 * elapsed.Seconds() / progress)
					remaining := clock.Until(t0.Add(predictedDuration)).Truncate(time.Second)
					bitsPerSecond := float64(stats.RestoredTotalFileSize) * bitsPerByte / elapsed.Seconds()
					if remaining > time.Second {
						maybeRemaining = fmt.Sprintf(" %v (%.1f%%) remaining %v", units.BitsPerSecondsString(bitsPerSecond), hundredPercent*progress, remaining)
					}
				}
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

func init() {
	addRestoreFlags(restoreCommand)
	restoreCommand.Action(repositoryReaderAction(runRestoreCommand))
}
