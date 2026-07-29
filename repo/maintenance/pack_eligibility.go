package maintenance

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

// packRetainReason explains why an unreferenced pack blob is not (yet)
// eligible for deletion. packEligible means it is.
type packRetainReason int

const (
	packEligible packRetainReason = iota
	packRetainedAfterCutoff
	packRetainedBelowMinAge
	packRetainedActiveSession
)

// packEligibility answers "would DeleteUnreferencedPacks delete this pack
// blob in this cycle?" for a blob that IterateUnreferencedPacks has already
// established is unreferenced.
//
// It exists so that DeleteUnreferencedPacks and extendBlobRetentionTime agree
// on that question. They have to: the extend step must not renew the
// retention of a pack that GC is about to reclaim, because a blob whose lock
// is refreshed on every maintenance cycle can never be deleted again, and the
// repository would then grow without bound. Sharing the predicate rather than
// re-deriving it is deliberate - two copies of these three conditions would
// drift, and the failure mode of drift here is silent, unbounded storage
// growth.
type packEligibility struct {
	cutoffTime     time.Time
	activeSessions map[content.SessionID]*content.SessionInfo
	safety         SafetyParameters
}

// newPackEligibility captures the state the decision depends on: the storage
// clock cutoff and the set of sessions that are still alive.
func newPackEligibility(ctx context.Context, rep repo.DirectRepositoryWriter, notAfterTime time.Time, safety SafetyParameters) (*packEligibility, error) {
	activeSessions, err := rep.ContentManager().ListActiveSessions(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load active sessions")
	}

	cutoffTime := notAfterTime
	if cutoffTime.IsZero() {
		cutoffTime = rep.Time()
	}

	// move the cutoff time a bit forward, because on Windows clock does not
	// reliably move forward so we may end up not deleting some blobs - this
	// only really affects tests, since BlobDeleteMinAge provides real
	// protection here.
	const cutoffTimeSlack = 1 * time.Second

	cutoffTime = cutoffTime.Add(cutoffTimeSlack)

	return &packEligibility{
		cutoffTime:     cutoffTime,
		activeSessions: activeSessions,
		safety:         safety,
	}, nil
}

// reason classifies bm. The zero value packEligible means the pack may be
// deleted; anything else names the condition that keeps it around.
func (e *packEligibility) reason(bm blob.Metadata) packRetainReason {
	if bm.Timestamp.After(e.cutoffTime) {
		return packRetainedAfterCutoff
	}

	if age := e.cutoffTime.Sub(bm.Timestamp); age < e.safety.PackDeleteMinAge {
		return packRetainedBelowMinAge
	}

	sid := content.SessionIDFromBlobID(bm.BlobID)
	if s, ok := e.activeSessions[sid]; ok {
		if age := e.cutoffTime.Sub(s.CheckpointTime); age < e.safety.SessionExpirationAge {
			return packRetainedActiveSession
		}
	}

	return packEligible
}

// age returns how old bm is relative to the cutoff, for logging.
func (e *packEligibility) age(bm blob.Metadata) time.Duration {
	return e.cutoffTime.Sub(bm.Timestamp)
}

// reclaimablePackBlobs returns the pack blobs that a GC pass would delete
// right now. Used by extendBlobRetentionTime to leave exactly those blobs
// alone.
func reclaimablePackBlobs(ctx context.Context, rep repo.DirectRepositoryWriter, parallel int, safety SafetyParameters) (map[blob.ID]struct{}, error) {
	if parallel <= 0 {
		parallel = 16
	}

	elig, err := newPackEligibility(ctx, rep, time.Time{}, safety)
	if err != nil {
		return nil, err
	}

	prefixes := []blob.ID{
		content.PackBlobIDPrefixRegular,
		content.PackBlobIDPrefixSpecial,
		content.BlobIDPrefixSession,
	}

	out := map[blob.ID]struct{}{}

	// IterateUnreferencedPacks invokes the callback from `parallel`
	// goroutines, so the map needs its own lock. The rest of the callback is
	// read-only.
	var mu sync.Mutex

	if err := rep.ContentManager().IterateUnreferencedPacks(ctx, prefixes, parallel, func(bm blob.Metadata) error {
		if elig.reason(bm) != packEligible {
			return nil
		}

		mu.Lock()
		out[bm.BlobID] = struct{}{}
		mu.Unlock()

		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error looking for reclaimable pack blobs")
	}

	return out, nil
}
