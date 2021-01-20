package maintenance

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

// DropDeletedContents rewrites indexes while dropping deleted contents above certain age.
func DropDeletedContents(ctx context.Context, rep repo.DirectRepositoryWriter, dropDeletedBefore time.Time) error {
	log(ctx).Infof("Dropping contents deleted before %v", dropDeletedBefore)

	return rep.ContentManager().CompactIndexes(ctx, content.CompactOptions{
		AllIndexes:        true,
		DropDeletedBefore: dropDeletedBefore,
	})
}
