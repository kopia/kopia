package maintenance

import "time"

// SafetyParameters specifies timing parameters that affect safety of maintenance.
type SafetyParameters struct {
	// Do not rewrite contents younger than this age.
	RewriteMinAge time.Duration

	// Snapshot GC: MinContentAgeSubjectToGC is the minimum age of content to be subject to garbage collection.
	MinContentAgeSubjectToGC time.Duration

	// MarginBetweenSnapshotGC is the minimal amount of time that must pass between snapshot
	// GC cycles to allow all in-flight snapshots during earlier GC to be flushed and
	// visible to a following GC. The uploader will automatically create a checkpoint every 45 minutes,
	// so ~1 hour should be enough but we're setting this to a higher conservative value for extra safety.
	MarginBetweenSnapshotGC time.Duration

	// RequireTwoGCCycles indicates that two GC cycles are required.
	RequireTwoGCCycles bool

	// DisableEventualConsistencySafety disables wait time to allow settling of eventually-consistent writes in blob stores.
	DisableEventualConsistencySafety bool

	// DropContentFromIndexExtraMargin is the amount of margin time before dropping deleted contents from indices.
	DropContentFromIndexExtraMargin time.Duration

	// Blob GC: Delete unused blobs above this age.
	BlobDeleteMinAge time.Duration

	// Blob GC: Drop incomplete session blobs above this age.
	SessionExpirationAge time.Duration

	// Minimum time that must pass after content rewrite before we delete orphaned blobs.
	MinRewriteToOrphanDeletionDelay time.Duration
}

// Supported safety levels.
//
//nolint:gochecknoglobals
var (
	// SafetyNone has safety parameters which allow full garbage collection without unnecessary
	// delays, but it is safe only if no other kopia clients are running and storage backend is
	// strongly consistent.
	SafetyNone = SafetyParameters{
		BlobDeleteMinAge:                 0,
		DropContentFromIndexExtraMargin:  0,
		MarginBetweenSnapshotGC:          0,
		MinContentAgeSubjectToGC:         0,
		RewriteMinAge:                    0,
		SessionExpirationAge:             0,
		RequireTwoGCCycles:               false,
		DisableEventualConsistencySafety: true,
	}

	// SafetyFull has default safety parameters which allow safe GC concurrent with snapshotting
	// by other Kopia clients.
	SafetyFull = SafetyParameters{
		BlobDeleteMinAge:                24 * time.Hour, //nolint:mnd
		DropContentFromIndexExtraMargin: time.Hour,
		MarginBetweenSnapshotGC:         4 * time.Hour,  //nolint:mnd
		MinContentAgeSubjectToGC:        24 * time.Hour, //nolint:mnd
		RewriteMinAge:                   2 * time.Hour,  //nolint:mnd
		SessionExpirationAge:            96 * time.Hour, //nolint:mnd
		RequireTwoGCCycles:              true,
		MinRewriteToOrphanDeletionDelay: time.Hour,
	}
)
