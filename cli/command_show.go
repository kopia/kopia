package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type commandShow struct {
	path string

	objectReaderOptionsFlags

	out textOutput
}

func (c *commandShow) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("show", "Displays the contents of a repository object.").Alias("cat")
	cmd.Arg("object-path", "Path").Required().StringVar(&c.path)
	c.objectReaderOptionsFlags.setup(cmd)
	cmd.Action(svc.repositoryReaderAction(c.run))

	c.out.setup(svc)
}

func (c *commandShow) run(ctx context.Context, rep repo.Repository) error {
	ctx = c.applyObjectReaderOptions(ctx)

	oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, c.path)
	if err != nil {
		return errors.Wrapf(err, "unable to parse ID: %v", c.path)
	}

	r, err := rep.OpenObject(ctx, oid)
	if err != nil {
		return errors.Wrapf(err, "error opening object %v", oid)
	}

	defer r.Close() //nolint:errcheck

	return errors.Wrap(iocopy.JustCopy(c.out.stdout(), r), "unable to copy data")
}
