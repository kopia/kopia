package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var cacheSyncCommand = cacheCommands.Command("sync", "Synchronizes the metadata cache with blobs in storage")

func runCacheSyncCommand(ctx context.Context, rep *repo.DirectRepository) error {
	return rep.Content.SyncMetadataCache(ctx)
}

func init() {
	cacheSyncCommand.Action(directRepositoryAction(runCacheSyncCommand))
}
