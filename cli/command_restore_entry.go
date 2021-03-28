package cli

import (
	"context"
	"math"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	restoreEntryCommandHelp = `Expand one or more levels of a directory or file from shallow
placeholder file (i.e. one with the .kopia-entry suffix.) For example

'restore-entry entry.kopia-entry'

will expand one level of 'entry.kopia-entry': replacing
'entry.kopia-entry' with the real entry. If 'entry.kopia-entry' is a
placeholder for a directory, it will recursively expand
'shallow-restore-at-depth' (default 0) levels below entry.
`
)

type commandRestoreEntry struct {
	commandRestore
	srDirEntryPath     string
}

func (c *commandRestoreEntry) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("restore-entry", restoreEntryCommandHelp).Alias("expand")
	cmd.Arg("entry-path", "A shallow placeholder file `*.kopia-entry`").Required().StringVar(&c.srDirEntryPath)
	c.addRestoreAndEntryFlags(cmd)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandRestoreEntry) restoreEntryOutput() (restore.Output, error) {
	targetpath := restore.PathIfPlaceholder(c.srDirEntryPath)
	if targetpath == "" {
		return nil, errors.Errorf("restore-entry requires a placeholder argument")
	}

	p, err := filepath.Abs(targetpath)
	if err != nil {
		return nil, errors.Wrapf(err, "can't find absolute %q", targetpath)
	}

	// TODO(rjk): this might be wrongs...
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
}

func (c *commandRestoreEntry) run(ctx context.Context, rep repo.Repository) error {
	_, oerr := c.restoreEntryOutput()
	if oerr != nil {
		return errors.Wrap(oerr, "unable to initialize output")
	}

	// rootEntry
	_, err := snapshotfs.GetEntryFromPlaceholder(ctx, rep, localfs.PlaceholderFilePath(c.srDirEntryPath))
	if err != nil {
		return errors.Wrapf(err, "unable to get filesystem entry for placeholder %q", c.srDirEntryPath)
	}

	if c.restoreShallowAtDepth == math.MaxInt32 {
		c.restoreShallowAtDepth = 0
	}

	// TODO(rjk): where did this go? (I think that I need to fix things up a bit.)
	// Where this went: I removed it entirely.
	// return c.runRestoreAndEntryCommand(ctx, rep, rootEntry, output)

	return nil
}
