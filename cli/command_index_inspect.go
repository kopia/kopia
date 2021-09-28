package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandIndexInspect struct {
	all     bool
	active  bool
	blobIDs []string

	contentIDs []string

	out textOutput
}

func (c *commandIndexInspect) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("inspect", "Inspect index blob")
	cmd.Flag("all", "Inspect all index blobs in the repository, including inactive").BoolVar(&c.all)
	cmd.Flag("active", "Inspect all active index blobs").BoolVar(&c.active)
	cmd.Flag("content-id", "Inspect all active index blobs").StringsVar(&c.contentIDs)
	cmd.Arg("blobs", "Names of index blobs to inspect").StringsVar(&c.blobIDs)
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandIndexInspect) run(ctx context.Context, rep repo.DirectRepository) error {
	switch {
	case c.all:
		return c.inspectAllBlobs(ctx, rep, true)
	case c.active:
		return c.inspectAllBlobs(ctx, rep, false)
	case len(c.blobIDs) > 0:
		for _, indexBlobID := range c.blobIDs {
			if err := c.inspectSingleIndexBlob(ctx, rep, blob.ID(indexBlobID)); err != nil {
				return err
			}
		}
	default:
		return errors.Errorf("must pass either --all, --active or provide a list of blob IDs to inspect")
	}

	return nil
}

func (c *commandIndexInspect) inspectAllBlobs(ctx context.Context, rep repo.DirectRepository, includeInactive bool) error {
	indexes, err := rep.IndexBlobs(ctx, includeInactive)
	if err != nil {
		return errors.Wrap(err, "error listing index blobs")
	}

	for _, bm := range indexes {
		if err := c.inspectSingleIndexBlob(ctx, rep, bm.BlobID); err != nil {
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

		if !c.shouldInclude(ci) {
			continue
		}

		c.out.printStdout("%v %v %v %v %v %v %v %v\n",
			formatTimestampPrecise(bm.Timestamp), bm.BlobID,
			ci.GetContentID(), state, formatTimestampPrecise(ci.Timestamp()), ci.GetPackBlobID(), ci.GetPackOffset(), ci.GetPackedLength())
	}
}

func (c *commandIndexInspect) shouldInclude(ci content.Info) bool {
	if len(c.contentIDs) == 0 {
		return true
	}

	contentID := string(ci.GetContentID())

	for _, cid := range c.contentIDs {
		if cid == contentID {
			return true
		}
	}

	return false
}

func (c *commandIndexInspect) inspectSingleIndexBlob(ctx context.Context, rep repo.DirectRepository, blobID blob.ID) error {
	log(ctx).Debugf("Inspecting blob %v...", blobID)

	bm, err := rep.BlobReader().GetMetadata(ctx, blobID)
	if err != nil {
		return errors.Wrapf(err, "unable to get metadata for %v", blobID)
	}

	var data gather.WriteBuffer
	defer data.Close()

	if err = rep.BlobReader().GetBlob(ctx, blobID, 0, -1, &data); err != nil {
		return errors.Wrapf(err, "unable to get data for %v", blobID)
	}

	entries, err := content.ParseIndexBlob(ctx, blobID, data.Bytes(), rep.Crypter())
	if err != nil {
		return errors.Wrapf(err, "unable to recover index from %v", blobID)
	}

	c.dumpIndexBlobEntries(bm, entries)

	return nil
}
