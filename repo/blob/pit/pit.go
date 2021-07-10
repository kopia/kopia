// Package pit provides a Point-in-time view of a versioned blob store
package pit

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
)

type pitStorage struct {
	blob.Storage

	vs          VersionedStorage
	pointInTime time.Time
}

// NewWrapper wraps s with a PiT store when s is versioned and pit is non-zero.
// Otherwise it returns s unmodified.
func NewWrapper(ctx context.Context, s blob.Storage, pointInTime *time.Time) (blob.Storage, error) {
	if pointInTime == nil || pointInTime.IsZero() {
		return s, nil
	}

	// Check if the bucket supports versioning when a point in time is specified
	if v, ok := s.(VersionedStorage); ok {
		switch versioned, err := v.IsVersioned(ctx); {
		case err != nil:
			return nil, errors.Wrap(err, "Coud not determine if the bucket is versioned")
		case !versioned:
			return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket store")
		}

		return readonly.NewWrapper(&pitStorage{
			Storage:     s,
			pointInTime: *pointInTime,
			vs:          v,
		}), nil
	}

	return s, nil
}
