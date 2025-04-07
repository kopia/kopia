package cli

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
)

type commandIndexInspect struct {
	all     bool
	active  bool
	blobIDs []string

	contentIDs []string
	parallel   int

	out textOutput
}

func (c *commandIndexInspect) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("inspect", "Inspect index blob")
	cmd.Flag("all", "Inspect all index blobs in the repository, including inactive").BoolVar(&c.all)
	cmd.Flag("active", "Inspect all active index blobs").BoolVar(&c.active)
	cmd.Flag("content-id", "Inspect all active index blobs").StringsVar(&c.contentIDs)
	cmd.Flag("parallel", "Parallelism").Default("8").IntVar(&c.parallel)
	cmd.Arg("blobs", "Names of index blobs to inspect").StringsVar(&c.blobIDs)
	cmd.Action(svc.directRepositoryReadAction(c.run))

	c.out.setup(svc)
}

func (c *commandIndexInspect) run(ctx context.Context, rep repo.DirectRepository) error {
	output := make(chan indexBlobPlusContentInfo)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		c.dumpIndexBlobEntries(output)
	}()

	err := c.runWithOutput(ctx, rep, output)
	close(output)
	wg.Wait()

	return err
}

func (c *commandIndexInspect) runWithOutput(ctx context.Context, rep repo.DirectRepository, output chan indexBlobPlusContentInfo) error {
	switch {
	case c.all:
		return c.inspectAllBlobs(ctx, rep, true, output)
	case c.active:
		return c.inspectAllBlobs(ctx, rep, false, output)
	case len(c.blobIDs) > 0:
		for _, indexBlobID := range c.blobIDs {
			if err := c.inspectSingleIndexBlob(ctx, rep, blob.ID(indexBlobID), output); err != nil {
				return err
			}
		}
	default:
		return errors.New("must pass either --all, --active or provide a list of blob IDs to inspect")
	}

	return nil
}

func (c *commandIndexInspect) inspectAllBlobs(ctx context.Context, rep repo.DirectRepository, includeInactive bool, output chan indexBlobPlusContentInfo) error {
	indexes, err := rep.IndexBlobs(ctx, includeInactive)
	if err != nil {
		return errors.Wrap(err, "error listing index blobs")
	}

	indexesCh := make(chan indexblob.Metadata, len(indexes))
	for _, bm := range indexes {
		indexesCh <- bm
	}

	close(indexesCh)

	var eg errgroup.Group

	for range c.parallel {
		eg.Go(func() error {
			for bm := range indexesCh {
				if err := c.inspectSingleIndexBlob(ctx, rep, bm.BlobID, output); err != nil {
					return err
				}
			}

			return nil
		})
	}

	//nolint:wrapcheck
	return eg.Wait()
}

func (c *commandIndexInspect) dumpIndexBlobEntries(entries chan indexBlobPlusContentInfo) {
	for ent := range entries {
		ci := ent.contentInfo
		bm := ent.indexBlob

		state := "created"
		if ci.Deleted {
			state = "deleted"
		}

		if !c.shouldInclude(ci) {
			continue
		}

		c.out.printStdout("%v %v %v %v %v %v %v %v\n",
			formatTimestampPrecise(bm.Timestamp), bm.BlobID,
			ci.ContentID, state, formatTimestampPrecise(ci.Timestamp()), ci.PackBlobID, ci.PackOffset, ci.PackedLength)
	}
}

func (c *commandIndexInspect) shouldInclude(ci content.Info) bool {
	if len(c.contentIDs) == 0 {
		return true
	}

	contentID := ci.ContentID.String()

	for _, cid := range c.contentIDs {
		if cid == contentID {
			return true
		}
	}

	return false
}

type indexBlobPlusContentInfo struct {
	indexBlob   blob.Metadata
	contentInfo content.Info
}

func (c *commandIndexInspect) inspectSingleIndexBlob(ctx context.Context, rep repo.DirectRepository, blobID blob.ID, output chan indexBlobPlusContentInfo) error {
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

	entries, err := content.ParseIndexBlob(blobID, data.Bytes(), rep.ContentReader().ContentFormat())
	if err != nil {
		return errors.Wrapf(err, "unable to recover index from %v", blobID)
	}

	for _, ent := range entries {
		output <- indexBlobPlusContentInfo{bm, ent}
	}

	return nil
}
