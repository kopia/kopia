package cli

import (
	"archive/zip"
	"compress/gzip"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
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

	restores []restoreSourceTarget
}

func (c *commandRestore) setup(svc appServices, parent commandParent) {
	c.restoreShallowAtDepth = unlimitedDepth

	cmd := parent.Command("restore", restoreCommandHelp)
	cmd.Arg("sources", restoreCommandSourcePathHelp).Required().StringsVar(&c.restoreTargetPaths)
	cmd.Flag("overwrite-directories", "Overwrite existing directories").Default("true").BoolVar(&c.restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").Default("true").BoolVar(&c.restoreOverwriteFiles)
	cmd.Flag("overwrite-symlinks", "Specifies whether or not to overwrite already existing symlinks").Default("true").BoolVar(&c.restoreOverwriteSymlinks)
	cmd.Flag("write-sparse-files", "When doing a restore, attempt to write files sparsely-allocating the minimum amount of disk space needed.").Default("false").BoolVar(&c.restoreWriteSparseFiles)
	cmd.Flag("consistent-attributes", "When multiple snapshots match, fail if they have inconsistent attributes").Envar("KOPIA_RESTORE_CONSISTENT_ATTRIBUTES").BoolVar(&c.restoreConsistentAttributes)
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
func (c *commandRestore) constructTargetPairs() error {
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
	case tplen == 0 && restpslen == 2:
		// This means that none of the restoreTargetPaths are placeholders and we
		// we have two args: a sourceID and a destination directory.
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
	return errors.Errorf("restore requires a source and targetpath or placeholders")
}

func (c *commandRestore) restoreOutput(ctx context.Context) (restore.Output, error) {
	err := c.constructTargetPairs()
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

		if err := o.Init(); err != nil {
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

func (c *commandRestore) run(ctx context.Context, rep repo.Repository) error {
	output, oerr := c.restoreOutput(ctx)
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
			re, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, rstp.source, c.restoreConsistentAttributes)
			if err != nil {
				return errors.Wrap(err, "unable to get filesystem entry")
			}

			rootEntry = re
		}

		eta := timetrack.Start()

		st, err := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
			Parallel:               c.restoreParallel,
			Incremental:            c.restoreIncremental,
			IgnoreErrors:           c.restoreIgnoreErrors,
			RestoreDirEntryAtDepth: c.restoreShallowAtDepth,
			MinSizeForPlaceholder:  c.minSizeForPlaceholder,
			ProgressCallback: func(ctx context.Context, stats restore.Stats) {
				restoredCount := stats.RestoredFileCount + stats.RestoredDirCount + stats.RestoredSymlinkCount + stats.SkippedCount
				enqueuedCount := stats.EnqueuedFileCount + stats.EnqueuedDirCount + stats.EnqueuedSymlinkCount

				if restoredCount == 0 {
					return
				}

				var maybeRemaining, maybeSkipped, maybeErrors string

				if est, ok := eta.Estimate(float64(stats.RestoredTotalFileSize), float64(stats.EnqueuedTotalFileSize)); ok {
					maybeRemaining = fmt.Sprintf(" %v (%.1f%%) remaining %v",
						units.BytesPerSecondsString(est.SpeedPerSecond),
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
	}

	return nil
}
