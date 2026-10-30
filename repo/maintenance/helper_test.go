package maintenance

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// helpers exported for tests

func ExtendBlobRetentionTime(ctx context.Context, rep repo.DirectRepositoryWriter, opt ExtendBlobRetentionTimeOptions) (*maintenancestats.ExtendBlobRetentionStats, error) {
	return extendBlobRetentionTime(ctx, rep, opt)
}
