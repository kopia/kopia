package blobtesting

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// RetentionStorage allows for better testing of retention and object
// locking-related code by allowing callers to get the retention parameters of
// the blob and attempt "modifying" the blob with TouchBlob.
type RetentionStorage interface {
	blob.Storage
	TouchBlob(ctx context.Context, id blob.ID, threshold time.Duration) (time.Time, error)
	GetRetention(ctx context.Context, id blob.ID) (blob.RetentionMode, time.Time, error)
}
