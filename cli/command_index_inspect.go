package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandIndexInspect struct {
	ids []string

	out textOutput
}

func (c *commandIndexInspect) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("inspect", "Inpect index blob")
	cmd.Arg("blobs", "Names of index blobs to inspect").StringsVar(&c.ids)
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandIndexInspect) run(ctx context.Context, rep repo.DirectRepository) error {
	for _, indexBlobID := range c.ids {
		if err := c.inspectSingleIndexBlob(ctx, rep, blob.ID(indexBlobID)); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandIndexInspect) dumpIndexBlobEntries(bm blob.Metadata, entries []content.Info) {
	for _, ci := range entries {
		state := "created"
		if ci.GetDeleted() {
			state = "deleted"
		}

		c.out.printStderr("%v %v %v %v %v %v %v %v\n",
			formatTimestampPrecise(bm.Timestamp), bm.BlobID,
			ci.GetContentID(), state, formatTimestampPrecise(ci.Timestamp()), ci.GetPackBlobID(), ci.GetPackOffset(), ci.GetPackedLength())
	}
}

func (c *commandIndexInspect) inspectSingleIndexBlob(ctx context.Context, rep repo.DirectRepository, blobID blob.ID) error {
	bm, err := rep.BlobReader().GetMetadata(ctx, blobID)
	if err != nil {
		return errors.Wrapf(err, "unable to get metadata for %v", blobID)
	}

	entries, err := rep.IndexBlobReader().ParseIndexBlob(ctx, blobID)
	if err != nil {
		return errors.Wrapf(err, "unable to recover index from %v", blobID)
	}

	c.dumpIndexBlobEntries(bm, entries)

	return nil
}
