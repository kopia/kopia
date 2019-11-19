package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const restoreCommandHelp = `Restore a directory from a snapshot into the specified target path.

The target path must not exist and it is created by the restore command. The case when the target is the root directory in the local machine is an exception, in this case no attempt is made to create the root directory and the contents of the source directory are copied to the local root directory.
The restore command conservatively refuses to overwrite previously existing files or directories.

The source to be restored is specified in the form of a directory ID and optionally a sub-directory path.

For example, the following source and target arguments will restore the contents of the 'kffbb7c28ea6c34d6cbe555d1cf80faa9' directory into a new, local directory named 'd1'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9 d1'

Similarly, the following command will restore the contents of a subdirectory 'subdir/subdir2' under 'kffbb7c28ea6c34d6cbe555d1cf80faa9'  into a new, local directory named 'sd2'

'restore kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2 sd2'
`

var (
	restoreCommand           = app.Command("restore", restoreCommandHelp)
	restoreCommandSourcePath = restoreCommand.Arg("source-path", "Source directory ID/path in the form of a directory ID and optionally a sub-directory path. For example, ' kffbb7c28ea6c34d6cbe555d1cf80faa9' or 'kffbb7c28ea6c34d6cbe555d1cf80faa9/subdir1/subdir2'").Required().String()
	restoreCommandTargetPath = restoreCommand.Arg("target-path", "Path of the directory for the contents to be restored").Required().String()
)

func runRestoreCommand(ctx context.Context, rep *repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *restoreCommandSourcePath)
	if err != nil {
		return err
	}

	return snapshotfs.Restore(ctx, rep, *restoreCommandTargetPath, oid)
}

func init() {
	restoreCommand.Action(repositoryAction(runRestoreCommand))
}
