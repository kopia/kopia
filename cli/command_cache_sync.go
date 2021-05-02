package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandCacheSync struct{}

func (c *commandCacheSync) setup(app appServices, parent commandParent) {
	cmd := parent.Command("sync", "Synchronizes the metadata cache with blobs in storage")
	cmd.Action(app.directRepositoryWriteAction(c.run))
}

func (c *commandCacheSync) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	return rep.ContentManager().SyncMetadataCache(ctx)
}
