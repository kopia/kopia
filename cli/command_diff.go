package cli

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"

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
	oid1, err := parseObjectID(ctx, rep, *diffFirstObjectPath)
	if err != nil {
		return err
	}

	oid2, err := parseObjectID(ctx, rep, *diffSecondObjectPath)
	if err != nil {
		return err
	}

	isDir1 := strings.HasPrefix(string(oid1), "k")
	isDir2 := strings.HasPrefix(string(oid2), "k")

	if isDir1 != isDir2 {
		return errors.New("arguments do diff must both be directories or both non-directories")
	}

	d, err := diff.NewComparer(os.Stdout)
	if err != nil {
		return err
	}
	defer d.Close() //nolint:errcheck

	if *diffCompareFiles {
		parts := strings.Split(*diffCommandCommand, " ")
		d.DiffCommand = parts[0]
		d.DiffArguments = parts[1:]
	}

	if isDir1 {
		return d.Compare(
			ctx,
			snapshotfs.DirectoryEntry(rep, oid1, nil),
			snapshotfs.DirectoryEntry(rep, oid2, nil),
		)
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
