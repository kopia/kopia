//go:build darwin || (linux && amd64)

// Package checker defines the framework for creating and restoring snapshots
// with a data integrity check
package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
)

const (
	deleteLimitEnvKey  = "LIVE_SNAP_DELETE_LIMIT"
	defaultDeleteLimit = 10
)

// Checker is an object that can take snapshots and restore them, performing
// a validation for data consistency.
type Checker struct {
	RestoreDir            string
	snapshotIssuer        robustness.Snapshotter
	snapshotMetadataStore robustness.Store
	RecoveryMode          bool
	DeleteLimit           int

	mu          sync.RWMutex
	SnapIDIndex snapmeta.Index // +checklocksignore
}

// NewChecker instantiates a new Checker, returning its pointer. A temporary
// directory is created to mount restored data.
func NewChecker(snapIssuer robustness.Snapshotter, snapmetaStore robustness.Store, restoreDir string) (*Checker, error) {
	restoreDir, err := os.MkdirTemp(restoreDir, "restore-data-")
	if err != nil {
		return nil, err
	}

	delLimitStr := os.Getenv(deleteLimitEnvKey)

	delLimit, err := strconv.Atoi(delLimitStr)
	if err != nil {
		log.Printf("using default delete limit %d", defaultDeleteLimit)
		delLimit = defaultDeleteLimit
	}

	return &Checker{
		RestoreDir:            restoreDir,
		snapshotIssuer:        snapIssuer,
		snapshotMetadataStore: snapmetaStore,
		RecoveryMode:          false,
		DeleteLimit:           delLimit,
		SnapIDIndex:           make(snapmeta.Index),
	}, nil
}

// Cleanup cleans up the Checker's temporary restore data directory.
func (chk *Checker) Cleanup() {
	if chk.RestoreDir != "" {
		os.RemoveAll(chk.RestoreDir) //nolint:errcheck
	}
}

// GetSnapIDs gets the list of snapshot IDs being tracked by the checker's snapshot store.
func (chk *Checker) GetSnapIDs() []string {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	return chk.SnapIDIndex.GetKeys(AllSnapshotsIdxName)
}

// SnapshotMetadata holds metadata associated with a given snapshot.
type SnapshotMetadata struct {
	SnapID         string    `json:"snapID"`
	SnapStartTime  time.Time `json:"snapStartTime"`
	SnapEndTime    time.Time `json:"snapEndTime"`
	DeletionTime   time.Time `json:"deletionTime"`
	ValidationData []byte    `json:"validationData"`
}

// IsDeleted returns true if the SnapshotMetadata references a snapshot ID that
// has been deleted.
func (ssMeta *SnapshotMetadata) IsDeleted() bool {
	return !ssMeta.DeletionTime.IsZero()
}

// GetSnapshotMetadata gets the metadata associated with the given snapshot ID.
func (chk *Checker) GetSnapshotMetadata(ctx context.Context, snapID string) (*SnapshotMetadata, error) {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	return chk.loadSnapshotMetadata(ctx, snapID)
}

// GetLiveSnapIDs gets the list of snapshot IDs being tracked by the checker's snapshot store
// that do not have a deletion time associated with them.
func (chk *Checker) GetLiveSnapIDs() []string {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	return chk.SnapIDIndex.GetKeys(LiveSnapshotsIdxName)
}

// IsSnapshotIDDeleted reports whether the metadata associated with the provided snapshot ID
// has it marked as deleted.
func (chk *Checker) IsSnapshotIDDeleted(ctx context.Context, snapID string) (bool, error) {
	md, err := chk.loadSnapshotMetadata(ctx, snapID)
	if err != nil {
		return false, err
	}

	return md.IsDeleted(), nil
}

// VerifySnapshotMetadata compares the list of live snapshot IDs present in
// the Checker's metadata against a list of live snapshot IDs in the connected
// repository. This should not be called concurrently, as there is no thread
// safety guaranteed.
func (chk *Checker) VerifySnapshotMetadata(ctx context.Context) error {
	// Get live snapshot metadata keys
	liveSnapsInMetadata := chk.GetLiveSnapIDs()

	// Get live snapshots listed in the repo itself
	liveSnapsInRepo, err := chk.snapshotIssuer.ListSnapshots(ctx)
	if err != nil {
		return err
	}

	metadataMap := make(map[string]struct{})
	for _, meta := range liveSnapsInMetadata {
		metadataMap[meta] = struct{}{}
	}

	liveMap := make(map[string]struct{})
	for _, live := range liveSnapsInRepo {
		liveMap[live] = struct{}{}
	}

	var errCount int

	for _, metaSnapID := range liveSnapsInMetadata {
		if _, ok := liveMap[metaSnapID]; !ok {
			log.Printf("Metadata present for snapID %v but not found in list of repo snapshots", metaSnapID)

			if chk.RecoveryMode {
				chk.mu.Lock()
				chk.snapshotMetadataStore.Delete(ctx, metaSnapID)
				chk.SnapIDIndex.RemoveFromIndex(metaSnapID, LiveSnapshotsIdxName)
				chk.mu.Unlock()
			} else {
				errCount++
			}
		}
	}

	var liveSnapsDeleted int

	for _, liveSnapID := range liveSnapsInRepo {
		if _, ok := metadataMap[liveSnapID]; ok {
			// Found live snapshot ID in the metadata. No recovery handling needed.
			continue
		}

		log.Printf("Live snapshot present for snapID %v but not found in known metadata", liveSnapID)

		if chk.RecoveryMode {
			if liveSnapsDeleted >= chk.DeleteLimit {
				log.Printf("delete limit (%v) reached", chk.DeleteLimit)

				errCount++
			}

			// Might as well delete the snapshot since we don't have metadata for it
			log.Printf("Deleting snapshot ID %s", liveSnapID)

			err = chk.snapshotIssuer.DeleteSnapshot(ctx, liveSnapID, map[string]string{})
			if err != nil {
				log.Printf("error deleting snapshot: %s", err)

				errCount++
			}

			liveSnapsDeleted++
		} else {
			errCount++
		}
	}

	if errCount > 0 {
		return errors.Errorf("hit %v errors verifying snapshot metadata", errCount)
	}

	return nil
}

// TakeSnapshot gathers state information on the requested snapshot path, then
// performs the snapshot action defined by the Checker's Snapshotter.
func (chk *Checker) TakeSnapshot(ctx context.Context, sourceDir string, opts map[string]string) (snapID string, err error) {
	snapID, fingerprint, stats, err := chk.snapshotIssuer.CreateSnapshot(ctx, sourceDir, opts)
	if err != nil {
		return snapID, err
	}

	ssMeta := &SnapshotMetadata{
		SnapID:         snapID,
		SnapStartTime:  stats.SnapStartTime,
		SnapEndTime:    stats.SnapEndTime,
		ValidationData: fingerprint,
	}

	chk.mu.Lock()
	defer chk.mu.Unlock()

	err = chk.saveSnapshotMetadata(ctx, ssMeta)
	if err != nil {
		return snapID, err
	}

	chk.SnapIDIndex.AddToIndex(snapID, AllSnapshotsIdxName)
	chk.SnapIDIndex.AddToIndex(snapID, LiveSnapshotsIdxName)

	return snapID, nil
}

// RestoreSnapshot restores a snapshot to the Checker's temporary restore directory
// using the Checker's Snapshotter, and performs a data consistency check on the
// resulting tree using the saved snapshot data.
func (chk *Checker) RestoreSnapshot(ctx context.Context, snapID string, reportOut io.Writer, opts map[string]string) error {
	// Make an independent directory for the restore
	restoreSubDir, err := os.MkdirTemp(chk.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID))
	if err != nil {
		return err
	}

	defer os.RemoveAll(restoreSubDir) //nolint:errcheck

	return chk.RestoreSnapshotToPath(ctx, snapID, restoreSubDir, reportOut, opts)
}

// RestoreSnapshotToPath restores a snapshot to the requested path
// using the Checker's Snapshotter, and performs a data consistency check on the
// resulting tree using the saved snapshot data.
func (chk *Checker) RestoreSnapshotToPath(ctx context.Context, snapID, destPath string, reportOut io.Writer, opts map[string]string) error {
	ssMeta, err := chk.safeRestorePrepare(ctx, snapID)
	if err != nil {
		return err
	}

	return chk.RestoreVerifySnapshot(ctx, snapID, destPath, ssMeta, reportOut, opts)
}

// safeRestorePrepare wraps key indexing operations that need to be
// executed together under one mutex lock.
func (chk *Checker) safeRestorePrepare(ctx context.Context, snapID string) (*SnapshotMetadata, error) {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	if !chk.SnapIDIndex.IsKeyInIndex(snapID, LiveSnapshotsIdxName) {
		// Preventing restore of a snapshot ID that has been (or is currently
		// being) deleted.
		log.Printf("Snapshot ID %s could not be found as a live snapshot", snapID)
		return nil, robustness.ErrNoOp
	}

	ssMeta, err := chk.loadSnapshotMetadata(ctx, snapID)
	if err != nil {
		return nil, err
	}

	return ssMeta, nil
}

// RestoreVerifySnapshot restores a snapshot and verifies its integrity against
// the metadata provided.
func (chk *Checker) RestoreVerifySnapshot(ctx context.Context, snapID, destPath string, ssMeta *SnapshotMetadata, reportOut io.Writer, opts map[string]string) error {
	if ssMeta != nil {
		return chk.snapshotIssuer.RestoreSnapshotCompare(ctx, snapID, destPath, ssMeta.ValidationData, reportOut, opts)
	}

	// We have no metadata for this snapshot ID.
	if !chk.RecoveryMode {
		return robustness.ErrMetadataMissing
	}

	// Recovery path:
	// If in recovery mode, restore the snapshot by snapshot ID and gather
	// its fingerprint (i.e. assume the snapshot will restore properly).
	fingerprint, err := chk.snapshotIssuer.RestoreSnapshot(ctx, snapID, destPath, opts)
	if err != nil {
		return err
	}

	ssMeta = &SnapshotMetadata{
		SnapID:         snapID,
		ValidationData: fingerprint,
	}

	chk.mu.Lock()
	defer chk.mu.Unlock()

	err = chk.saveSnapshotMetadata(ctx, ssMeta)
	if err != nil {
		return err
	}

	chk.SnapIDIndex.AddToIndex(snapID, AllSnapshotsIdxName)
	chk.SnapIDIndex.AddToIndex(snapID, LiveSnapshotsIdxName)

	return nil
}

// Index names for categorizing snapshot lookups.
const (
	DeletedSnapshotsIdxName = "deleted-snapshots-idx"
	LiveSnapshotsIdxName    = "live-snapshots-idx"
	AllSnapshotsIdxName     = "all-snapshots-idx"
)

// DeleteSnapshot performs the Snapshotter's DeleteSnapshot action, and
// marks the snapshot with the given snapshot ID as deleted.
func (chk *Checker) DeleteSnapshot(ctx context.Context, snapID string, opts map[string]string) error {
	// Load the metadata, ensure the snapshot hasn't already been deleted.
	// If not, remove it from the list of live snapshots. These two tasks must
	// be done atomically.
	ssMeta, err := chk.safeDeletePrepare(ctx, snapID)
	if err != nil {
		return err
	}

	err = chk.snapshotIssuer.DeleteSnapshot(ctx, snapID, opts)
	if err != nil {
		return err
	}

	ssMeta.DeletionTime = clock.Now()
	ssMeta.ValidationData = nil

	return chk.safeDeleteFinish(ctx, ssMeta)
}

// safeDeletePrepare is a routine that will remove the snapshot ID from
// the live snapshots index if the snapshot has not already been deleted.
// The check for deletion must be done atomically with the removal from the
// index to ensure no races, which could lead to false failures if the Delete
// operation runs against a snapshot that has already been deleted.
func (chk *Checker) safeDeletePrepare(ctx context.Context, snapID string) (*SnapshotMetadata, error) {
	chk.mu.Lock()
	defer chk.mu.Unlock()

	if !chk.SnapIDIndex.IsKeyInIndex(snapID, LiveSnapshotsIdxName) {
		// Preventing restore of a snapshot ID that has been already deleted.
		log.Printf("Cannot delete snapshot ID %s as it is not a live snapshot", snapID)
		return nil, robustness.ErrNoOp
	}

	ssMeta, err := chk.loadSnapshotMetadata(ctx, snapID)
	if err != nil {
		return nil, err
	}

	// Remove the snapshot ID from the live snapshots index - no concurrent
	// calls to find a live snapshot ID will find this snapshot ID.
	// Place the snapshot ID in the list of IDs with unknown state. If we
	// hit an error during delete, we don't know whether to expect it to
	// still be valid or not.
	chk.SnapIDIndex.RemoveFromIndex(snapID, LiveSnapshotsIdxName)

	return ssMeta, nil
}

// safeDelete finish will atomically update the metadata repository with the
// deleted snapshot metadata and add the ID to the deleted snapshots index.
func (chk *Checker) safeDeleteFinish(ctx context.Context, ssMeta *SnapshotMetadata) error {
	chk.mu.Lock()
	defer chk.mu.Unlock()

	chk.SnapIDIndex.AddToIndex(ssMeta.SnapID, DeletedSnapshotsIdxName)

	err := chk.saveSnapshotMetadata(ctx, ssMeta)
	if err != nil {
		return err
	}

	return nil
}

func (chk *Checker) saveSnapshotMetadata(ctx context.Context, ssMeta *SnapshotMetadata) error {
	ssMetaRaw, err := json.Marshal(ssMeta)
	if err != nil {
		return err
	}

	err = chk.snapshotMetadataStore.Store(ctx, ssMeta.SnapID, ssMetaRaw)
	if err != nil {
		return err
	}

	return nil
}

func (chk *Checker) loadSnapshotMetadata(ctx context.Context, snapID string) (*SnapshotMetadata, error) {
	// Lookup metadata by snapshot ID
	b, err := chk.snapshotMetadataStore.Load(ctx, snapID)
	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, errors.Errorf("could not find snapID %v", snapID)
	}

	ssMeta := &SnapshotMetadata{}

	err = json.Unmarshal(b, ssMeta)
	if err != nil {
		return nil, err
	}

	return ssMeta, nil
}
