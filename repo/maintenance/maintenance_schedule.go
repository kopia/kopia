package maintenance

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

const maintenanceScheduleBlobID = "kopia.schedule"

// Schedule keeps track of scheduled maintenance times.
type Schedule struct {
	NextFullMaintenanceTime  time.Time `json:"nextFullMaintenance"`
	NextQuickMaintenanceTime time.Time `json:"nextQuickMaintenance"`
}

// GetSchedule gets the scheduled maintenance times.
func GetSchedule(ctx context.Context, rep MaintainableRepository) (*Schedule, error) {
	v, err := rep.BlobStorage().GetBlob(ctx, maintenanceScheduleBlobID, 0, -1)
	if err == blob.ErrBlobNotFound {
		return &Schedule{}, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, "error reading schedule blob")
	}

	s := &Schedule{}
	if err := json.Unmarshal(v, s); err != nil {
		return nil, errors.Wrap(err, "malformed schedule blob")
	}

	return s, nil
}

// SetSchedule updates scheduled maintenance times.
func SetSchedule(ctx context.Context, rep MaintainableRepository, s *Schedule) error {
	v, err := json.Marshal(s)
	if err != nil {
		return errors.Wrap(err, "unable to serialize JSON")
	}

	return rep.BlobStorage().PutBlob(ctx, maintenanceScheduleBlobID, gather.FromSlice(v))
}
