package cli

import (
	"context"
	"math"
	"path/filepath"

	"github.com/alecthomas/kingpin"
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

var (
	restoryEntryCommand = app.Command("restore-entry", restoreEntryCommandHelp).Alias("expand")
	srDirEntryPath      = ""
)

// addShallowRestoreFlags sets up command line flags for the
// shallowrestore command on the kingpin command framework.
func addShallowRestoreFlags(cmd *kingpin.CmdClause) {
	cmd.Arg("entry-path", "A shallow placeholder file `*.kopia-entry`").Required().StringVar(&srDirEntryPath)

	// Add all of the necessary state from restore.
	addRestoreAndEntryFlags(cmd)
}

func restoreEntryOutput() (restore.Output, error) {
	targetpath := restore.PathIfPlaceholder(srDirEntryPath)
	if targetpath == "" {
		return nil, errors.Errorf("restore-entry requires a placeholder argument")
	}

	p, err := filepath.Abs(targetpath)
	if err != nil {
		return nil, errors.Wrapf(err, "can't find absolute %q", targetpath)
	}

	return makeFileSystemOutput(p), nil
}

func runRestoreEntryCommand(ctx context.Context, rep repo.Repository) error {
	output, oerr := restoreEntryOutput()
	if oerr != nil {
		return errors.Wrap(oerr, "unable to initialize output")
	}

	rootEntry, err := snapshotfs.GetEntryFromPlaceholder(ctx, rep, localfs.PlaceholderFilePath(srDirEntryPath))
	if err != nil {
		return errors.Wrapf(err, "unable to get filesystem entry for placeholder %q", srDirEntryPath)
	}

	if restoreShallowAtDepth == math.MaxInt32 {
		restoreShallowAtDepth = 0
	}

	return runRestoreAndEntryCommand(ctx, rep, rootEntry, output)
}

func init() {
	addShallowRestoreFlags(restoryEntryCommand)
	restoryEntryCommand.Action(repositoryReaderAction(runRestoreEntryCommand))
}
