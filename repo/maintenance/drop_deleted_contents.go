package maintenance

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content/indexblob"
)

// DropDeletedContents rewrites indexes while dropping deleted contents above certain age.
func DropDeletedContents(ctx context.Context, rep repo.DirectRepositoryWriter, dropDeletedBefore time.Time, safety SafetyParameters) error {
	log(ctx).Infof("Dropping contents deleted before %v", dropDeletedBefore)

	//nolint:wrapcheck
	return rep.ContentManager().CompactIndexes(ctx, indexblob.CompactOptions{
		AllIndexes:                       true,
		DropDeletedBefore:                dropDeletedBefore,
		DisableEventualConsistencySafety: safety.DisableEventualConsistencySafety,
	})
}
