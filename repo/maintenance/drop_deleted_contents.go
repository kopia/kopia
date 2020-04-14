package maintenance

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

const defaultDropDeletedContentsAge = 24 * time.Hour

// DropDeletedContentOptions specifies options for DropDeletedContent maintenance.
type DropDeletedContentOptions struct {
	MinDeletedAge time.Duration `json:"minDeletedAge"`
}

// DropDeletedContents rewrites indexes while dropping deleted contents above certain age.
func DropDeletedContents(ctx context.Context, rep MaintainableRepository, opt *DropDeletedContentOptions) error {
	if opt == nil {
		return errors.Errorf("options must be set")
	}

	if opt.MinDeletedAge <= 0 {
		opt.MinDeletedAge = defaultDropDeletedContentsAge
	}

	log(ctx).Infof("Dropping deleted contents older than %v", opt.MinDeletedAge)

	return rep.ContentManager().CompactIndexes(ctx, content.CompactOptions{
		AllIndexes:           true,
		SkipDeletedOlderThan: opt.MinDeletedAge,
	})
}
