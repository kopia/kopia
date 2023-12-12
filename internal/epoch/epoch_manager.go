// Package epoch manages repository epochs.
// It implements protocol described https://github.com/kopia/kopia/issues/1090 and is intentionally
// separate from 'content' package to be able to test in isolation.
package epoch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/completeset"
	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// LatestEpoch represents the current epoch number in GetCompleteIndexSet.
const LatestEpoch = -1

const (
	initiaRefreshAttemptSleep      = 100 * time.Millisecond
	maxRefreshAttemptSleep         = 15 * time.Second
	maxRefreshAttemptSleepExponent = 1.5
)

// ParametersProvider provides epoch manager parameters.
type ParametersProvider interface {
	GetParameters() (*Parameters, error)
}

// ErrVerySlowIndexWrite is returned by WriteIndex if a write takes more than 2 epochs (usually >48h).
// This is theoretically possible with laptops going to sleep, etc.
var ErrVerySlowIndexWrite = errors.Errorf("extremely slow index write - index write took more than two epochs")

// Parameters encapsulates all parameters that influence the behavior of epoch manager.
//
// Note as a historical mistake, JSON tags are not camelCase, but rather PascalCase. We can't change
// that since the parameters are stored in a repository.
type Parameters struct {
	// whether epoch manager is enabled, must be true.
	Enabled bool `json:"Enabled"`

	// how frequently each client will list blobs to determine the current epoch.
	EpochRefreshFrequency time.Duration `json:"EpochRefreshFrequency"`

	// number of epochs between full checkpoints.
	FullCheckpointFrequency int `json:"FullCheckpointFrequency"`

	// do not delete uncompacted blobs if the corresponding compacted blob age is less than this.
	CleanupSafetyMargin time.Duration `json:"CleanupSafetyMargin"`

	// minimum duration of an epoch
	MinEpochDuration time.Duration `json:"MinEpochDuration"`

	// advance epoch if number of files exceeds this
	EpochAdvanceOnCountThreshold int `json:"EpochAdvanceOnCountThreshold"`

	// advance epoch if total size of files exceeds this.
	EpochAdvanceOnTotalSizeBytesThreshold int64 `json:"EpochAdvanceOnTotalSizeBytesThreshold"`

	// number of blobs to delete in parallel during cleanup
	DeleteParallelism int `json:"DeleteParallelism"`
}

// GetEpochManagerEnabled returns whether epoch manager is enabled, must be true.
func (p *Parameters) GetEpochManagerEnabled() bool {
	return p.Enabled
}

// GetEpochRefreshFrequency determines how frequently each client will list blobs to determine the current epoch.
func (p *Parameters) GetEpochRefreshFrequency() time.Duration {
	return p.EpochRefreshFrequency
}

// GetEpochFullCheckpointFrequency returns the number of epochs between full checkpoints.
func (p *Parameters) GetEpochFullCheckpointFrequency() int {
	return p.FullCheckpointFrequency
}

// GetEpochCleanupSafetyMargin returns safety margin to prevent uncompacted blobs from being deleted if the corresponding compacted blob age is less than this.
func (p *Parameters) GetEpochCleanupSafetyMargin() time.Duration {
	return p.CleanupSafetyMargin
}

// GetMinEpochDuration returns the minimum duration of an epoch.
func (p *Parameters) GetMinEpochDuration() time.Duration {
	return p.MinEpochDuration
}

// GetEpochAdvanceOnCountThreshold returns the number of files above which epoch should be advanced.
func (p *Parameters) GetEpochAdvanceOnCountThreshold() int {
	return p.EpochAdvanceOnCountThreshold
}

// GetEpochAdvanceOnTotalSizeBytesThreshold returns the total size of files above which the epoch should be advanced.
func (p *Parameters) GetEpochAdvanceOnTotalSizeBytesThreshold() int64 {
	return p.EpochAdvanceOnTotalSizeBytesThreshold
}

// GetEpochDeleteParallelism returns the number of blobs to delete in parallel during cleanup.
func (p *Parameters) GetEpochDeleteParallelism() int {
	return p.DeleteParallelism
}

// Validate validates epoch parameters.
//
//nolint:gomnd
func (p *Parameters) Validate() error {
	if !p.Enabled {
		return nil
	}

	if p.MinEpochDuration < 10*time.Minute {
		return errors.Errorf("minimum epoch duration too low: %v", p.MinEpochDuration)
	}

	if p.EpochRefreshFrequency*3 > p.MinEpochDuration {
		return errors.New("epoch refresh period is too long, must be 1/3 of minimal epoch duration or shorter")
	}

	if p.FullCheckpointFrequency <= 0 {
		return errors.New("invalid epoch checkpoint frequency")
	}

	if p.CleanupSafetyMargin < p.EpochRefreshFrequency*3 {
		return errors.New("invalid cleanup safety margin, must be at least 3x epoch refresh frequency")
	}

	if p.EpochAdvanceOnCountThreshold < 10 {
		return errors.New("epoch advance on count too low")
	}

	if p.EpochAdvanceOnTotalSizeBytesThreshold < 1<<20 {
		return errors.New("epoch advance on size too low")
	}

	return nil
}

// DefaultParameters contains default epoch manager parameters.
//
//nolint:gomnd
func DefaultParameters() Parameters {
	return Parameters{
		Enabled:                               true,
		EpochRefreshFrequency:                 20 * time.Minute,
		FullCheckpointFrequency:               7,
		CleanupSafetyMargin:                   4 * time.Hour,
		MinEpochDuration:                      24 * time.Hour,
		EpochAdvanceOnCountThreshold:          20,
		EpochAdvanceOnTotalSizeBytesThreshold: 10 << 20,
		DeleteParallelism:                     4,
	}
}

// CurrentSnapshot captures a point-in time snapshot of a repository indexes, including current epoch
// information and compaction set.
type CurrentSnapshot struct {
	WriteEpoch                 int                     `json:"writeEpoch"`
	UncompactedEpochSets       map[int][]blob.Metadata `json:"unsettled"`
	LongestRangeCheckpointSets []*RangeMetadata        `json:"longestRangeCheckpointSets"`
	SingleEpochCompactionSets  map[int][]blob.Metadata `json:"singleEpochCompactionSets"`
	EpochStartTime             map[int]time.Time       `json:"epochStartTimes"`
	DeletionWatermark          time.Time               `json:"deletionWatermark"`
	ValidUntil                 time.Time               `json:"validUntil"`             // time after which the contents of this struct are no longer valid
	EpochMarkerBlobs           []blob.Metadata         `json:"epochMarkers"`           // list of epoch markers
	DeletionWatermarkBlobs     []blob.Metadata         `json:"deletionWatermarkBlobs"` // list of deletion watermark blobs
}

func (cs *CurrentSnapshot) isSettledEpochNumber(epoch int) bool {
	return epoch <= cs.WriteEpoch-numUnsettledEpochs
}

// Manager manages repository epochs.
type Manager struct {
	paramProvider ParametersProvider

	st       blob.Storage
	compact  CompactionFunc
	log      logging.Logger
	timeFunc func() time.Time

	allowCleanupWritesOnIndexLoad bool

	// wait group that waits for all compaction and cleanup goroutines.
	backgroundWork sync.WaitGroup

	// mutable under lock, data invalid until refresh succeeds at least once.
	mu             sync.Mutex
	lastKnownState CurrentSnapshot

	// counters keeping track of the number of times operations were too slow and had to
	// be retried, for testability.
	committedStateRefreshTooSlow *int32
	getCompleteIndexSetTooSlow   *int32
	writeIndexTooSlow            *int32
}

// Index blob prefixes.
const (
	EpochManagerIndexUberPrefix = "x"

	EpochMarkerIndexBlobPrefix      blob.ID = EpochManagerIndexUberPrefix + "e"
	UncompactedIndexBlobPrefix      blob.ID = EpochManagerIndexUberPrefix + "n"
	SingleEpochCompactionBlobPrefix blob.ID = EpochManagerIndexUberPrefix + "s"
	RangeCheckpointIndexBlobPrefix  blob.ID = EpochManagerIndexUberPrefix + "r"
	DeletionWatermarkBlobPrefix     blob.ID = EpochManagerIndexUberPrefix + "w"
)

// FirstEpoch is the number of the first epoch in a repository.
const FirstEpoch = 0

const numUnsettledEpochs = 2

// CompactionFunc merges the given set of index blobs into a new index blob set with a given prefix
// and writes them out as a set following naming convention established in 'complete_set.go'.
type CompactionFunc func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error

// Flush waits for all in-process compaction work to complete.
func (e *Manager) Flush() {
	// ensure all background compactions complete.
	e.backgroundWork.Wait()
}

// Current retrieves current snapshot.
func (e *Manager) Current(ctx context.Context) (CurrentSnapshot, error) {
	return e.committedState(ctx, 0)
}

// AdvanceDeletionWatermark moves the deletion watermark time to a given timestamp
// this causes all deleted content entries before given time to be treated as non-existent.
func (e *Manager) AdvanceDeletionWatermark(ctx context.Context, ts time.Time) error {
	cs, err := e.committedState(ctx, 0)
	if err != nil {
		return err
	}

	if ts.Before(cs.DeletionWatermark) {
		e.log.Debugf("ignoring attempt to move deletion watermark time backwards (%v < %v)", ts.Format(time.RFC3339), cs.DeletionWatermark.Format(time.RFC3339))

		return nil
	}

	blobID := blob.ID(fmt.Sprintf("%v%v", string(DeletionWatermarkBlobPrefix), ts.Unix()))

	if err := e.st.PutBlob(ctx, blobID, gather.FromSlice([]byte("deletion-watermark")), blob.PutOptions{}); err != nil {
		return errors.Wrap(err, "error writing deletion watermark")
	}

	e.Invalidate()

	return nil
}

// Refresh refreshes information about current epoch.
func (e *Manager) Refresh(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.refreshLocked(ctx)
}

// Derive a storage clock from the most recent timestamp among uncompacted index
// blobs, which are expected to have been recently written to the repository.
// When cleaning up blobs, only unreferenced blobs that are old enough relative to
// this max time will be removed. Blobs that are created relatively recently are
// preserved because other clients may have not observed them yet.
// Note: This assumes that storage clock moves forward somewhat reasonably.
func (e *Manager) maxCleanupTime(cs CurrentSnapshot) time.Time {
	var maxTime time.Time

	for _, v := range cs.UncompactedEpochSets {
		for _, bm := range v {
			if bm.Timestamp.After(maxTime) {
				maxTime = bm.Timestamp
			}
		}
	}

	return maxTime
}

func (e *Manager) cleanupInternal(ctx context.Context, cs CurrentSnapshot, p *Parameters) error {
	eg, ctx := errgroup.WithContext(ctx)

	// find max timestamp recently written to the repository to establish storage clock.
	// we will be deleting blobs whose timestamps are sufficiently old enough relative
	// to this max time. This assumes that storage clock moves forward somewhat reasonably.
	maxTime := e.maxCleanupTime(cs)
	if maxTime.IsZero() {
		return nil
	}

	// only delete blobs if a suitable replacement exists and has been written sufficiently
	// long ago. we don't want to delete blobs that are created too recently, because other clients
	// may have not observed them yet.
	maxReplacementTime := maxTime.Add(-p.CleanupSafetyMargin)

	eg.Go(func() error {
		return e.cleanupEpochMarkers(ctx, cs)
	})

	eg.Go(func() error {
		return e.cleanupWatermarks(ctx, cs, p, maxReplacementTime)
	})

	return errors.Wrap(eg.Wait(), "error cleaning up index blobs")
}

func (e *Manager) cleanupEpochMarkers(ctx context.Context, cs CurrentSnapshot) error {
	// delete epoch markers for epoch < current-1
	var toDelete []blob.ID

	for _, bm := range cs.EpochMarkerBlobs {
		if n, ok := epochNumberFromBlobID(bm.BlobID); ok {
			if n < cs.WriteEpoch-1 {
				toDelete = append(toDelete, bm.BlobID)
			}
		}
	}

	p, err := e.getParameters()
	if err != nil {
		return err
	}

	return errors.Wrap(blob.DeleteMultiple(ctx, e.st, toDelete, p.DeleteParallelism), "error deleting index blob marker")
}

func (e *Manager) cleanupWatermarks(ctx context.Context, cs CurrentSnapshot, p *Parameters, maxReplacementTime time.Time) error {
	var toDelete []blob.ID

	for _, bm := range cs.DeletionWatermarkBlobs {
		ts, ok := deletionWatermarkFromBlobID(bm.BlobID)
		if !ok {
			continue
		}

		if ts.Equal(cs.DeletionWatermark) {
			continue
		}

		if bm.Timestamp.Before(maxReplacementTime) {
			toDelete = append(toDelete, bm.BlobID)
		}
	}

	return errors.Wrap(blob.DeleteMultiple(ctx, e.st, toDelete, p.DeleteParallelism), "error deleting watermark blobs")
}

// CleanupSupersededIndexes cleans up the indexes which have been superseded by compacted ones.
func (e *Manager) CleanupSupersededIndexes(ctx context.Context) error {
	cs, err := e.committedState(ctx, 0)
	if err != nil {
		return err
	}

	p, err := e.getParameters()
	if err != nil {
		return err
	}

	// find max timestamp recently written to the repository to establish storage clock.
	// we will be deleting blobs whose timestamps are sufficiently old enough relative
	// to this max time. This assumes that storage clock moves forward somewhat reasonably.
	maxTime := e.maxCleanupTime(cs)
	if maxTime.IsZero() {
		return nil
	}

	// only delete blobs if a suitable replacement exists and has been written sufficiently
	// long ago. we don't want to delete blobs that are created too recently, because other clients
	// may have not observed them yet.
	maxReplacementTime := maxTime.Add(-p.CleanupSafetyMargin)

	e.log.Debugw("Cleaning up superseded index blobs...",
		"maxReplacementTime", maxReplacementTime)

	// delete uncompacted indexes for epochs that already have single-epoch compaction
	// that was written sufficiently long ago.
	blobs, err := blob.ListAllBlobs(ctx, e.st, UncompactedIndexBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error listing uncompacted blobs")
	}

	var toDelete []blob.ID

	for _, bm := range blobs {
		if epoch, ok := epochNumberFromBlobID(bm.BlobID); ok {
			if blobSetWrittenEarlyEnough(cs.SingleEpochCompactionSets[epoch], maxReplacementTime) {
				toDelete = append(toDelete, bm.BlobID)
			}
		}
	}

	if err := blob.DeleteMultiple(ctx, e.st, toDelete, p.DeleteParallelism); err != nil {
		return errors.Wrap(err, "unable to delete uncompacted blobs")
	}

	return nil
}

func blobSetWrittenEarlyEnough(replacementSet []blob.Metadata, maxReplacementTime time.Time) bool {
	max := blob.MaxTimestamp(replacementSet)
	if max.IsZero() {
		return false
	}

	// compaction set was written sufficiently long ago to be reliably discovered by all
	// other clients - we can delete uncompacted blobs for this epoch.
	return blob.MaxTimestamp(replacementSet).Before(maxReplacementTime)
}

func (e *Manager) getParameters() (*Parameters, error) {
	emp, err := e.paramProvider.GetParameters()
	if err != nil {
		return nil, errors.Wrap(err, "epoch manager parameters")
	}

	return emp, nil
}

func (e *Manager) refreshLocked(ctx context.Context) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "refreshLocked")
	}

	p, err := e.getParameters()
	if err != nil {
		return err
	}

	nextDelayTime := initiaRefreshAttemptSleep

	if !p.Enabled {
		return errors.Errorf("epoch manager not enabled")
	}

	for err := e.refreshAttemptLocked(ctx); err != nil; err = e.refreshAttemptLocked(ctx) {
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "refreshAttemptLocked")
		}

		e.log.Debugf("refresh attempt failed: %v, sleeping %v before next retry", err, nextDelayTime)
		time.Sleep(nextDelayTime)

		nextDelayTime = time.Duration(float64(nextDelayTime) * maxRefreshAttemptSleepExponent)

		if nextDelayTime > maxRefreshAttemptSleep {
			nextDelayTime = maxRefreshAttemptSleep
		}
	}

	return e.maybeCompactAndCleanupLocked(ctx, p)
}

func (e *Manager) maybeCompactAndCleanupLocked(ctx context.Context, p *Parameters) error {
	if !e.allowWritesOnLoad() {
		e.log.Debug("not performing epoch index cleanup")

		return nil
	}

	cs := e.lastKnownState

	if shouldAdvance(cs.UncompactedEpochSets[cs.WriteEpoch], p.MinEpochDuration, p.EpochAdvanceOnCountThreshold, p.EpochAdvanceOnTotalSizeBytesThreshold) {
		if err := e.advanceEpochMarker(ctx, cs); err != nil {
			return errors.Wrap(err, "error advancing epoch")
		}
	}

	e.maybeGenerateNextRangeCheckpointAsync(ctx, cs, p)
	e.maybeStartCleanupAsync(ctx, cs, p)
	e.maybeOptimizeRangeCheckpointsAsync(ctx, cs)

	return nil
}

// allowWritesOnLoad returns whether writes for index cleanup operations,
// such as index compaction, can be done during index reads.
// These index cleanup operations are disabled when using read-only storage
// since they will fail when they try to mutate the underlying storage.
func (e *Manager) allowWritesOnLoad() bool {
	return e.allowCleanupWritesOnIndexLoad && !e.st.IsReadOnly()
}

func (e *Manager) loadWriteEpoch(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, EpochMarkerIndexBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading write epoch")
	}

	for epoch, bm := range groupByEpochNumber(blobs) {
		cs.EpochStartTime[epoch] = bm[0].Timestamp

		if epoch > cs.WriteEpoch {
			cs.WriteEpoch = epoch
		}
	}

	cs.EpochMarkerBlobs = blobs

	return nil
}

func (e *Manager) loadDeletionWatermark(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, DeletionWatermarkBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading write epoch")
	}

	for _, b := range blobs {
		t, ok := deletionWatermarkFromBlobID(b.BlobID)
		if !ok {
			e.log.Debugf("ignoring malformed deletion watermark: %v", b.BlobID)
			continue
		}

		if t.After(cs.DeletionWatermark) {
			cs.DeletionWatermark = t
		}
	}

	cs.DeletionWatermarkBlobs = blobs

	return nil
}

func (e *Manager) loadRangeCheckpoints(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, RangeCheckpointIndexBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading full checkpoints")
	}

	e.log.Debugf("ranges: %v", blobs)

	var rangeCheckpointSets []*RangeMetadata

	for epoch1, m := range groupByEpochRanges(blobs) {
		for epoch2, bms := range m {
			if comp := completeset.FindFirst(bms); comp != nil {
				erm := &RangeMetadata{
					MinEpoch: epoch1,
					MaxEpoch: epoch2,
					Blobs:    comp,
				}

				rangeCheckpointSets = append(rangeCheckpointSets, erm)
			}
		}
	}

	cs.LongestRangeCheckpointSets = findLongestRangeCheckpoint(rangeCheckpointSets)

	return nil
}

func (e *Manager) loadSingleEpochCompactions(ctx context.Context, cs *CurrentSnapshot) error {
	blobs, err := blob.ListAllBlobs(ctx, e.st, SingleEpochCompactionBlobPrefix)
	if err != nil {
		return errors.Wrap(err, "error loading single-epoch compactions")
	}

	for epoch, bms := range groupByEpochNumber(blobs) {
		if comp := completeset.FindFirst(bms); comp != nil {
			cs.SingleEpochCompactionSets[epoch] = comp
		}
	}

	return nil
}

func (e *Manager) maybeGenerateNextRangeCheckpointAsync(ctx context.Context, cs CurrentSnapshot, p *Parameters) {
	latestSettled := cs.WriteEpoch - numUnsettledEpochs
	if latestSettled < 0 {
		return
	}

	firstNonRangeCompacted := 0
	if len(cs.LongestRangeCheckpointSets) > 0 {
		firstNonRangeCompacted = cs.LongestRangeCheckpointSets[len(cs.LongestRangeCheckpointSets)-1].MaxEpoch + 1
	}

	if latestSettled-firstNonRangeCompacted < p.FullCheckpointFrequency {
		e.log.Debugf("not generating range checkpoint")

		return
	}

	e.log.Debugf("generating range checkpoint")

	e.backgroundWork.Add(1)

	// we're starting background work, ignore parent cancellation signal.
	ctxutil.GoDetached(ctx, func(ctx context.Context) {
		defer e.backgroundWork.Done()

		if err := e.generateRangeCheckpointFromCommittedState(ctx, cs, firstNonRangeCompacted, latestSettled); err != nil {
			e.log.Errorf("unable to generate full checkpoint: %v, performance will be affected", err)
		}
	})
}

func (e *Manager) maybeOptimizeRangeCheckpointsAsync(ctx context.Context, cs CurrentSnapshot) {
	// TODO: implement me
	_ = cs
}

func (e *Manager) maybeStartCleanupAsync(ctx context.Context, cs CurrentSnapshot, p *Parameters) {
	e.backgroundWork.Add(1)

	// we're starting background work, ignore parent cancellation signal.
	ctxutil.GoDetached(ctx, func(ctx context.Context) {
		defer e.backgroundWork.Done()

		if err := e.cleanupInternal(ctx, cs, p); err != nil {
			e.log.Errorf("error cleaning up index blobs: %v, performance may be affected", err)
		}
	})
}

func (e *Manager) loadUncompactedEpochs(ctx context.Context, min, max int) (map[int][]blob.Metadata, error) {
	var mu sync.Mutex

	result := map[int][]blob.Metadata{}

	eg, ctx := errgroup.WithContext(ctx)

	for n := min; n <= max; n++ {
		n := n
		if n < 0 {
			continue
		}

		eg.Go(func() error {
			bm, err := blob.ListAllBlobs(ctx, e.st, UncompactedEpochBlobPrefix(n))
			if err != nil {
				return errors.Wrapf(err, "error listing uncompacted epoch %v", n)
			}

			mu.Lock()
			defer mu.Unlock()

			result[n] = bm
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "error listing uncompacted epochs")
	}

	return result, nil
}

// refreshAttemptLocked attempts to load the committedState of
// the index and updates `lastKnownState` state atomically when complete.
func (e *Manager) refreshAttemptLocked(ctx context.Context) error {
	e.log.Debug("refreshAttemptLocked")

	p, perr := e.getParameters()
	if perr != nil {
		return perr
	}

	cs := CurrentSnapshot{
		WriteEpoch:                0,
		EpochStartTime:            map[int]time.Time{},
		UncompactedEpochSets:      map[int][]blob.Metadata{},
		SingleEpochCompactionSets: map[int][]blob.Metadata{},
		ValidUntil:                e.timeFunc().Add(p.EpochRefreshFrequency),
	}

	eg, ctx1 := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return e.loadWriteEpoch(ctx1, &cs)
	})
	eg.Go(func() error {
		return e.loadDeletionWatermark(ctx1, &cs)
	})
	eg.Go(func() error {
		return e.loadSingleEpochCompactions(ctx1, &cs)
	})
	eg.Go(func() error {
		return e.loadRangeCheckpoints(ctx1, &cs)
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error refreshing")
	}

	ues, err := e.loadUncompactedEpochs(ctx, cs.WriteEpoch-1, cs.WriteEpoch+1)
	if err != nil {
		return errors.Wrap(err, "error loading uncompacted epochs")
	}

	cs.UncompactedEpochSets = ues

	e.log.Debugf("current epoch %v, uncompacted epoch sets %v %v %v, valid until %v",
		cs.WriteEpoch,
		len(ues[cs.WriteEpoch-1]),
		len(ues[cs.WriteEpoch]),
		len(ues[cs.WriteEpoch+1]),
		cs.ValidUntil.Format(time.RFC3339Nano))

	if now := e.timeFunc(); now.After(cs.ValidUntil) {
		atomic.AddInt32(e.committedStateRefreshTooSlow, 1)

		return errors.Errorf("refreshing committed state took too long (now %v, valid until %v)", now, cs.ValidUntil.Format(time.RFC3339Nano))
	}

	e.lastKnownState = cs

	return nil
}

func (e *Manager) advanceEpochMarker(ctx context.Context, cs CurrentSnapshot) error {
	blobID := blob.ID(fmt.Sprintf("%v%v", string(EpochMarkerIndexBlobPrefix), cs.WriteEpoch+1))

	if err := e.st.PutBlob(ctx, blobID, gather.FromSlice([]byte("epoch-marker")), blob.PutOptions{}); err != nil {
		return errors.Wrap(err, "error writing epoch marker")
	}

	return nil
}

func (e *Manager) committedState(ctx context.Context, ensureMinTime time.Duration) (CurrentSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if now := e.timeFunc().Add(ensureMinTime); now.After(e.lastKnownState.ValidUntil) {
		e.log.Debugf("refreshing committed state because it's no longer valid (now %v, valid until %v)", now, e.lastKnownState.ValidUntil.Format(time.RFC3339Nano))

		if err := e.refreshLocked(ctx); err != nil {
			return CurrentSnapshot{}, err
		}
	}

	return e.lastKnownState, nil
}

// GetCompleteIndexSet returns the set of blobs forming a complete index set up to the provided epoch number.
func (e *Manager) GetCompleteIndexSet(ctx context.Context, maxEpoch int) ([]blob.Metadata, time.Time, error) {
	for {
		cs, err := e.committedState(ctx, 0)
		if err != nil {
			return nil, time.Time{}, err
		}

		if maxEpoch == LatestEpoch {
			maxEpoch = cs.WriteEpoch + 1
		}

		result, err := e.getCompleteIndexSetForCommittedState(ctx, cs, 0, maxEpoch)
		if e.timeFunc().Before(cs.ValidUntil) {
			e.log.Debugf("Complete Index Set for [%v..%v]: %v, deletion watermark %v", 0, maxEpoch, blob.IDsFromMetadata(result), cs.DeletionWatermark)
			return result, cs.DeletionWatermark, err
		}

		// We need to retry if local process took too long (e.g. because the machine went
		// to sleep at the wrong moment) and committed state is no longer valid.
		//
		// One scenario where this matters is cleanup: if determining the set of indexes takes
		// too long, it's possible for a cleanup process to delete some of the uncompacted
		// indexes that are still treated as authoritative according to old committed state.
		//
		// Retrying will re-examine the state of the world and re-do the logic.
		e.log.Debugf("GetCompleteIndexSet took too long, retrying to ensure correctness")
		atomic.AddInt32(e.getCompleteIndexSetTooSlow, 1)
	}
}

var errWriteIndexTryAgain = errors.Errorf("try again")

// WriteIndex writes new index blob by picking the appropriate prefix based on current epoch.
func (e *Manager) WriteIndex(ctx context.Context, dataShards map[blob.ID]blob.Bytes) ([]blob.Metadata, error) {
	written := map[blob.ID]blob.Metadata{}
	writtenForEpoch := -1

	for {
		e.log.Debug("WriteIndex")

		p, err := e.getParameters()
		if err != nil {
			return nil, err
		}

		// make sure we have at least 75% of remaining time
		//nolint:gomnd
		cs, err := e.committedState(ctx, 3*p.EpochRefreshFrequency/4)
		if err != nil {
			return nil, errors.Wrap(err, "error getting committed state")
		}

		if cs.WriteEpoch != writtenForEpoch {
			if err = e.deletePartiallyWrittenShards(ctx, written); err != nil {
				return nil, errors.Wrap(err, "unable to delete partially written shard")
			}

			writtenForEpoch = cs.WriteEpoch
			written = map[blob.ID]blob.Metadata{}
		}

		err = e.writeIndexShards(ctx, dataShards, written, cs)
		if errors.Is(err, errWriteIndexTryAgain) {
			continue
		}

		if err != nil {
			e.log.Debugw("index-write-error", "error", err)
			return nil, err
		}

		e.Invalidate()

		break
	}

	cs, err := e.committedState(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "error getting committed state")
	}

	if cs.WriteEpoch >= writtenForEpoch+2 {
		e.log.Debugw("index-write-extremely-slow")

		if err = e.deletePartiallyWrittenShards(ctx, written); err != nil {
			e.log.Debugw("index-write-extremely-slow-cleanup-failed", "error", err)
		}

		return nil, ErrVerySlowIndexWrite
	}

	var results []blob.Metadata

	for _, v := range written {
		results = append(results, v)
	}

	e.log.Debugw("index-write-success", "results", results)

	return results, nil
}

func (e *Manager) deletePartiallyWrittenShards(ctx context.Context, blobs map[blob.ID]blob.Metadata) error {
	for blobID := range blobs {
		if err := e.st.DeleteBlob(ctx, blobID); err != nil {
			return errors.Wrap(err, "error deleting partially written shard")
		}
	}

	return nil
}

func (e *Manager) writeIndexShards(ctx context.Context, dataShards map[blob.ID]blob.Bytes, written map[blob.ID]blob.Metadata, cs CurrentSnapshot) error {
	e.log.Debugw("writing index shards",
		"shardCount", len(dataShards),
		"validUntil", cs.ValidUntil,
		"remaining", cs.ValidUntil.Sub(e.timeFunc()))

	for unprefixedBlobID, data := range dataShards {
		blobID := UncompactedEpochBlobPrefix(cs.WriteEpoch) + unprefixedBlobID
		if _, ok := written[blobID]; ok {
			e.log.Debugw("already written",
				"blobID", blobID)
			continue
		}

		if now := e.timeFunc(); !now.Before(cs.ValidUntil) {
			e.log.Debugw("write was too slow, retrying",
				"validUntil", cs.ValidUntil)
			atomic.AddInt32(e.writeIndexTooSlow, 1)

			return errWriteIndexTryAgain
		}

		bm, err := blob.PutBlobAndGetMetadata(ctx, e.st, blobID, data, blob.PutOptions{})
		if err != nil {
			return errors.Wrap(err, "error writing index blob")
		}

		e.log.Debugw("wrote-index-shard", "metadata", bm)

		written[bm.BlobID] = bm
	}

	return nil
}

// Invalidate ensures that all cached index information is discarded.
func (e *Manager) Invalidate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.lastKnownState = CurrentSnapshot{}
}

func (e *Manager) getCompleteIndexSetForCommittedState(ctx context.Context, cs CurrentSnapshot, minEpoch, maxEpoch int) ([]blob.Metadata, error) {
	var result []blob.Metadata

	startEpoch := minEpoch

	for _, c := range cs.LongestRangeCheckpointSets {
		if c.MaxEpoch > startEpoch {
			result = append(result, c.Blobs...)
			startEpoch = c.MaxEpoch + 1
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	e.log.Debugf("adding incremental state for epochs %v..%v on top of %v", startEpoch, maxEpoch, result)
	cnt := maxEpoch - startEpoch + 1

	tmp := make([][]blob.Metadata, cnt)

	for i := 0; i < cnt; i++ {
		i := i
		ep := i + startEpoch

		eg.Go(func() error {
			s, err := e.getIndexesFromEpochInternal(ctx, cs, ep)
			if err != nil {
				return errors.Wrapf(err, "error getting indexes for epoch %v", ep)
			}

			tmp[i] = s

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "error getting indexes")
	}

	for _, v := range tmp {
		result = append(result, v...)
	}

	return result, nil
}

func (e *Manager) getIndexesFromEpochInternal(ctx context.Context, cs CurrentSnapshot, epoch int) ([]blob.Metadata, error) {
	// check if the epoch is old enough to possibly have compacted blobs
	epochSettled := cs.isSettledEpochNumber(epoch)
	if epochSettled && cs.SingleEpochCompactionSets[epoch] != nil {
		return cs.SingleEpochCompactionSets[epoch], nil
	}

	// load uncompacted blobs for this epoch, reusing UncompactedEpochSets if we have it.
	uncompactedBlobs, ok := cs.UncompactedEpochSets[epoch]
	if !ok {
		ue, err := blob.ListAllBlobs(ctx, e.st, UncompactedEpochBlobPrefix(epoch))
		if err != nil {
			return nil, errors.Wrapf(err, "error listing uncompacted indexes for epoch %v", epoch)
		}

		uncompactedBlobs = ue
	}

	if epochSettled && e.allowWritesOnLoad() {
		e.backgroundWork.Add(1)

		// we're starting background work, ignore parent cancellation signal.
		ctxutil.GoDetached(ctx, func(ctx context.Context) {
			defer e.backgroundWork.Done()

			e.log.Debugf("starting single-epoch compaction of %v", epoch)

			if err := e.compact(ctx, blob.IDsFromMetadata(uncompactedBlobs), compactedEpochBlobPrefix(epoch)); err != nil {
				e.log.Errorf("unable to compact blobs for epoch %v: %v, performance will be affected", epoch, err)
			}
		})
	}

	// return uncompacted blobs to the caller while we're compacting them in background
	return uncompactedBlobs, nil
}

func (e *Manager) generateRangeCheckpointFromCommittedState(ctx context.Context, cs CurrentSnapshot, minEpoch, maxEpoch int) error {
	e.log.Debugf("generating range checkpoint for %v..%v", minEpoch, maxEpoch)

	completeSet, err := e.getCompleteIndexSetForCommittedState(ctx, cs, minEpoch, maxEpoch)
	if err != nil {
		return errors.Wrap(err, "unable to get full checkpoint")
	}

	if e.timeFunc().After(cs.ValidUntil) {
		return errors.Errorf("not generating full checkpoint - the committed state is no longer valid")
	}

	if err := e.compact(ctx, blob.IDsFromMetadata(completeSet), rangeCheckpointBlobPrefix(minEpoch, maxEpoch)); err != nil {
		return errors.Wrap(err, "unable to compact blobs")
	}

	return nil
}

// UncompactedEpochBlobPrefix returns the prefix for uncompacted blobs of a given epoch.
func UncompactedEpochBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", UncompactedIndexBlobPrefix, epoch))
}

func compactedEpochBlobPrefix(epoch int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_", SingleEpochCompactionBlobPrefix, epoch))
}

func rangeCheckpointBlobPrefix(epoch1, epoch2 int) blob.ID {
	return blob.ID(fmt.Sprintf("%v%v_%v_", RangeCheckpointIndexBlobPrefix, epoch1, epoch2))
}

// NewManager creates new epoch manager.
func NewManager(st blob.Storage, paramProvider ParametersProvider, compactor CompactionFunc, log logging.Logger, timeNow func() time.Time) *Manager {
	return &Manager{
		st:                           st,
		log:                          log,
		compact:                      compactor,
		timeFunc:                     timeNow,
		paramProvider:                paramProvider,
		getCompleteIndexSetTooSlow:   new(int32),
		committedStateRefreshTooSlow: new(int32),
		writeIndexTooSlow:            new(int32),
	}
}
