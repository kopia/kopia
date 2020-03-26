package cli

import (
	"context"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	restoreCommandHelp = `Restore a directory from a snapshot into the specified target path.

By default, the target path will be created by the restore command if it does
not exist.

The source to be restored is specified in the form of a directory ID and
optionally a sub-directory path.

For example, the following source and target arguments will restore the contents
of the 'kffbb7c28ea6c34d6cbe555d1cf80faa9' directory into a new, local directory
named 'd1'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9 d1'

Similarly, the following command will restore the contents of a subdirectory
'subdir/subdir2' under 'kffbb7c28ea6c34d6cbe555d1cf80faa9'  into a new, local
directory named 'sd2'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2 sd2'
`
	restoreCommandSourcePathHelp = `Source directory ID/path in the form of a
directory ID and optionally a sub-directory path. For example,
'kffbb7c28ea6c34d6cbe555d1cf80faa9' or
'kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2'
`
)

var (
	restoreCommand           = app.Command("restore", restoreCommandHelp)
	restoreCommandSourcePath = restoreCommand.Arg("source-path", restoreCommandSourcePathHelp).Required().String()
	restoreCommandTargetPath = restoreCommand.Arg("target-path", "Path of the directory for the contents to be restored").Required().String()

	restoreOverwriteDirectories = true
	restoreOverwriteFiles       = true
)

func addRestoreFlags(cmd *kingpin.CmdClause) {
	cmd.Flag("overwrite-directories", "Overwrite existing directories").BoolVar(&restoreOverwriteDirectories)
	cmd.Flag("overwrite-files", "Specifies whether or not to overwrite already existing files").
		BoolVar(&restoreOverwriteFiles)
}

func restoreOptions() localfs.CopyOptions {
	return localfs.CopyOptions{
		OverwriteDirectories: restoreOverwriteDirectories,
		OverwriteFiles:       restoreOverwriteFiles,
	}
}

func runRestoreCommand(ctx context.Context, rep repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *restoreCommandSourcePath)
	if err != nil {
		return err
	}

	return snapshotfs.RestoreRoot(ctx, rep, *restoreCommandTargetPath, oid, restoreOptions())
}

func init() {
	addRestoreFlags(restoreCommand)
	restoreCommand.Action(repositoryAction(runRestoreCommand))
}
