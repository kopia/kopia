package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandCacheSync struct{}

func (c *commandCacheSync) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("sync", "Synchronizes the metadata cache with blobs in storage")
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandCacheSync) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	// nolint:wrapcheck
	return rep.ContentManager().SyncMetadataCache(ctx)
}
