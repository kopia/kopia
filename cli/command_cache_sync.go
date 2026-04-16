package cli

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandCacheSync struct {
	parallel int
}

func (c *commandCacheSync) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("sync", "Synchronizes the metadata cache with blobs in storage")
	cmd.Flag("parallel", "Fetch parallelism").Default("16").IntVar(&c.parallel)
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandCacheSync) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	eg, ctx := errgroup.WithContext(ctx)

	ch := make(chan blob.ID, c.parallel)

	// workers that will prefetch blobs.
	for range c.parallel {
		eg.Go(func() error {
			for blobID := range ch {
				if err := rep.ContentManager().MetadataCache().PrefetchBlob(ctx, blobID); err != nil {
					return errors.Wrap(err, "error prefetching blob")
				}
			}

			return nil
		})
	}

	// populate channel with blob IDs.
	eg.Go(func() error {
		defer close(ch)

		return rep.BlobReader().ListBlobs(ctx, content.PackBlobIDPrefixSpecial, func(bm blob.Metadata) error {
			ch <- bm.BlobID

			return nil
		})
	})

	return errors.Wrap(eg.Wait(), "error synchronizing metadata cache")
}
