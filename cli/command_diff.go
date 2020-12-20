package cli

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	diffCommand          = app.Command("diff", "Displays differences between two repository objects (files or directories)").Alias("compare")
	diffFirstObjectPath  = diffCommand.Arg("object-path1", "First object/path").Required().String()
	diffSecondObjectPath = diffCommand.Arg("object-path2", "Second object/path").Required().String()
	diffCompareFiles     = diffCommand.Flag("files", "Compare files by launching diff command for all pairs of (old,new)").Short('f').Bool()
	diffCommandCommand   = diffCommand.Flag("diff-command", "Displays differences between two repository objects (files or directories)").Default(defaultDiffCommand()).Envar("KOPIA_DIFF").String()
)

func runDiffCommand(ctx context.Context, rep repo.Repository) error {
	ent1, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, *diffFirstObjectPath, false)
	if err != nil {
		return errors.Wrapf(err, "error getting filesystem entry for %v", *diffFirstObjectPath)
	}

	ent2, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, *diffSecondObjectPath, false)
	if err != nil {
		return errors.Wrapf(err, "error getting filesystem entry for %v", *diffSecondObjectPath)
	}

	_, isDir1 := ent1.(fs.Directory)
	_, isDir2 := ent2.(fs.Directory)

	if isDir1 != isDir2 {
		return errors.New("arguments do diff must both be directories or both non-directories")
	}

	d, err := diff.NewComparer(os.Stdout)
	if err != nil {
		return errors.Wrap(err, "error creating comparer")
	}
	defer d.Close() //nolint:errcheck

	if *diffCompareFiles {
		parts := strings.Split(*diffCommandCommand, " ")
		d.DiffCommand = parts[0]
		d.DiffArguments = parts[1:]
	}

	if isDir1 {
		return d.Compare(ctx, ent1, ent2)
	}

	return errors.New("comparing files not implemented yet")
}

func defaultDiffCommand() string {
	if isWindows() {
		return "cmp"
	}

	return "diff -u"
}

func init() {
	diffCommand.Action(repositoryAction(runDiffCommand))
}
