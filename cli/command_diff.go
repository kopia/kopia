package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandDiff struct {
	diffFirstObjectPath  string
	diffSecondObjectPath string
	diffCompareFiles     bool
	diffCommandCommand   string

	out textOutput
}

func (c *commandDiff) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("diff", "Displays differences between two repository objects (files or directories)").Alias("compare")
	cmd.Arg("object-path1", "First object/path").Required().StringVar(&c.diffFirstObjectPath)
	cmd.Arg("object-path2", "Second object/path").Required().StringVar(&c.diffSecondObjectPath)
	cmd.Flag("files", "Compare files by launching diff command for all pairs of (old,new)").Short('f').BoolVar(&c.diffCompareFiles)
	cmd.Flag("diff-command", "Displays differences between two repository objects (files or directories)").Default(defaultDiffCommand()).Envar(svc.EnvName("KOPIA_DIFF")).StringVar(&c.diffCommandCommand)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commandDiff) run(ctx context.Context, rep repo.Repository) error {
	ent1, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, c.diffFirstObjectPath, false)
	if err != nil {
		return errors.Wrapf(err, "error getting filesystem entry for %v", c.diffFirstObjectPath)
	}

	ent2, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, rep, c.diffSecondObjectPath, false)
	if err != nil {
		return errors.Wrapf(err, "error getting filesystem entry for %v", c.diffSecondObjectPath)
	}

	_, isDir1 := ent1.(fs.Directory)
	_, isDir2 := ent2.(fs.Directory)

	if isDir1 != isDir2 {
		return errors.New("arguments do diff must both be directories or both non-directories")
	}

	d, err := diff.NewComparer(c.out.stdout())
	if err != nil {
		return errors.Wrap(err, "error creating comparer")
	}
	defer d.Close() //nolint:errcheck

	if c.diffCompareFiles {
		parts := strings.Split(c.diffCommandCommand, " ")
		d.DiffCommand = parts[0]
		d.DiffArguments = parts[1:]
	}

	if isDir1 {
		return errors.Wrap(d.Compare(ctx, ent1, ent2), "error comparing directories")
	}

	return errors.New("comparing files not implemented yet")
}

func defaultDiffCommand() string {
	if isWindows() {
		return "cmp"
	}

	return "diff -u"
}
