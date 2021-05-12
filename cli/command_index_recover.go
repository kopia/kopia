package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandIndexRecover struct {
	blobIDs []string
	commit  bool

	svc appServices
}

func (c *commandIndexRecover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("recover", "Recover indexes from pack blobs")
	cmd.Flag("blobs", "Names of pack blobs to recover from (default=all packs)").StringsVar(&c.blobIDs)
	cmd.Flag("commit", "Commit recovered content").BoolVar(&c.commit)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandIndexRecover) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	c.svc.advancedCommand(ctx)

	var totalCount int

	defer func() {
		if totalCount == 0 {
			log(ctx).Infof("No blocks recovered.")
			return
		}

		if !c.commit {
			log(ctx).Infof("Found %v blocks to recover, but not committed. Re-run with --commit", totalCount)
		} else {
			log(ctx).Infof("Recovered %v blocks.", totalCount)
		}
	}()

	if len(c.blobIDs) == 0 {
		for _, prefix := range content.PackBlobIDPrefixes {
			err := rep.BlobStorage().ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
				c.recoverIndexFromSinglePackFile(ctx, rep, bm.BlobID, bm.Length, &totalCount)
				return nil
			})
			if err != nil {
				return errors.Wrapf(err, "recovering indexes from prefix %q", prefix)
			}
		}
	}

	for _, packFile := range c.blobIDs {
		c.recoverIndexFromSinglePackFile(ctx, rep, blob.ID(packFile), 0, &totalCount)
	}

	return nil
}

func (c *commandIndexRecover) recoverIndexFromSinglePackFile(ctx context.Context, rep repo.DirectRepositoryWriter, blobID blob.ID, length int64, totalCount *int) {
	recovered, err := rep.ContentManager().RecoverIndexFromPackBlob(ctx, blobID, length, c.commit)
	if err != nil {
		log(ctx).Errorf("unable to recover index from %v: %v", blobID, err)
		return
	}

	*totalCount += len(recovered)
	log(ctx).Infof("Recovered %v entries from %v (commit=%v)", len(recovered), blobID, c.commit)
}
