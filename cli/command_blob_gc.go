package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandBlobGC struct {
	delete   string
	parallel int
	prefix   string
	safety   maintenance.SafetyParameters

	svc appServices
}

func (c *commandBlobGC) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("gc", "Garbage-collect unused blobs")
	cmd.Flag("delete", "Whether to delete unused blobs").StringVar(&c.delete)
	cmd.Flag("parallel", "Number of parallel blob scans").Default("16").IntVar(&c.parallel)
	cmd.Flag("prefix", "Only GC blobs with given prefix").StringVar(&c.prefix)
	safetyFlagVar(cmd, &c.safety)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandBlobGC) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	c.svc.advancedCommand(ctx)

	opts := maintenance.DeleteUnreferencedBlobsOptions{
		DryRun:   c.delete != "yes",
		Parallel: c.parallel,
		Prefix:   blob.ID(c.prefix),
	}

	n, err := maintenance.DeleteUnreferencedBlobs(ctx, rep, opts, c.safety)
	if err != nil {
		return errors.Wrap(err, "error deleting unreferenced blobs")
	}

	if opts.DryRun && n > 0 {
		log(ctx).Info("Pass --delete=yes to delete.")
	}

	return nil
}
