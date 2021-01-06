package cli

import (
	"context"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	shallowRestoreCommandHelp = `Restore one level of a directory or file from a snapshot into the
specified target path where the subsequent directory levels and files
will be represented by placeholder files with the .kopiadir or
.kopiafile suffix as appropriate. Placeholder files will snapshot as
if they were contents of the repository from which they were restored.
This permits modifying a restored file tree without round-tripping
files that do not require alteration.

By default, the target path will be created by the shallowrestore
command if it does not exist.

The source to be restored is specified in the form of a directory or
file ID and optionally a sub-directory path or a shallow placeholder
file.

For example, the following source and target arguments will
shallowrestore the contents of the 'kffbb7c28ea6c34d6cbe555d1cf80faa9'
directory into a new, local directory named 'd1'

'shallowrestore kffbb7c28ea6c34d6cbe555d1cf80faa9 d1'

Similarly, the following command will shallowrestore the contents of a
subdirectory 'subdir/subdir2' under
'kffbb7c28ea6c34d6cbe555d1cf80faa9' into a new, local directory named
'sd2'

'shallowrestore kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2 sd2'

Shallow placeholder files unambiguously specify a previously backed up
file or tree in the repository and can be restored unambiguously. For
example:

'shallowrestore d3.kopiadir'

will remove the d3.kopiadir placeholder and shallowrestore the
referenced repository contents into path d3. Similarly for files:

'shallowrestore f3.kopiafile'

`
)

type commandShallowRestore struct {
	srSourceID string
	srTargetPath string
	shallowRestoreParallel int
}

// addShallowRestoreFlags sets up command line flags for the
// shallowrestore command on the kingpin command framework.
func (c *commandShallowRestore) setup(svc appServices, parent commandParent) {
	cmd  := parent.Command("shallowrestore", "Does a shallowrestore of object-path.").Alias("shallow")
	cmd.Arg("source", shallowRestoreCommandHelp).Required().StringVar(&c.srSourceID)
	cmd.Arg("target-path", "Path of the directory for the contents to be restored").StringVar(&c.srTargetPath)
	cmd.Flag("parallel", "Shallow restore parallelism (1=disable)").IntVar(&c.shallowRestoreParallel)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandShallowRestore) shallowrestoreOutput() (restore.Output, error) {
	targetpath, placeholderrestore := restore.PathIfPlaceholder(c.srSourceID)
	if !placeholderrestore {
		if c.srTargetPath == "" {
			return nil, errors.Errorf("restore requires a target-path unless shallow-restoring a placeholder")
		}

		targetpath = c.srTargetPath
	}

	p, err := filepath.Abs(targetpath)
	if err != nil {
		return nil, errors.Wrapf(err, "can't find absolute %q", targetpath)
	}

	return &restore.ShallowFilesystemOutput{
		FilesystemOutput: restore.FilesystemOutput{
			TargetPath:             p,
			OverwriteDirectories:   true,
			OverwriteFiles:         true,
			IgnorePermissionErrors: true,
			SkipOwners:             false,
			SkipPermissions:        false,
			SkipTimes:              false,
		},
	}, nil
}

func (c *commandShallowRestore) run(ctx context.Context, rep repo.Repository) error {
	output, oerr := c.shallowrestoreOutput()
	if oerr != nil {
		return errors.Wrap(oerr, "unable to initialize output")
	}

	var rootEntry fs.Entry

	if _, placeholderrestore := restore.PathIfPlaceholder(c.srSourceID); placeholderrestore {
		re, err := snapshotfs.GetEntryFromPlaceholder(ctx, rep, localfs.PlaceholderFilePath(c.srSourceID))
		if err != nil {
			return errors.Wrapf(err, "unable to get filesystem entry for placeholder %q", c.srSourceID)
		}

		rootEntry = re
	} else {
		re, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, c.srSourceID, false)
		if err != nil {
			return errors.Wrap(err, "unable to get filesystem entry")
		}

		rootEntry = re
	}

	st, rerr := restore.Entry(ctx, rep, output, rootEntry, restore.Options{
		Parallel: c.shallowRestoreParallel,
		// TODO(rjk): Consider supporting depths greater than 1.
		MaxDepth: 1,
		ProgressCallback: func(ctx context.Context, stats restore.Stats) {
			// TODO(rjk): Add progress logging when depth is configurable.
		},
	})
	if rerr != nil {
		return errors.Wrap(rerr, "shallow restoring failed")
	}

	printRestoreStats(ctx, st)

	return nil
}
