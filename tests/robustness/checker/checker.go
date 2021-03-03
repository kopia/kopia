// Package checker defines the framework for creating and restoring snapshots
// with a data integrity check
package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
	validator             robustness.Comparer
	RecoveryMode          bool
	DeleteLimit           int

	mu          sync.RWMutex
	SnapIDIndex snapmeta.Index
}

// NewChecker instantiates a new Checker, returning its pointer. A temporary
// directory is created to mount restored data.
func NewChecker(snapIssuer robustness.Snapshotter, snapmetaStore robustness.Store, validator robustness.Comparer, restoreDir string) (*Checker, error) {
	restoreDir, err := ioutil.TempDir(restoreDir, "restore-data-")
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
		validator:             validator,
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

	return chk.SnapIDIndex.GetKeys(allSnapshotsIdxName)
}

// SnapshotMetadata holds metadata associated with a given snapshot.
type SnapshotMetadata struct {
	SnapID         string    `json:"snapID"`
	SnapStartTime  time.Time `json:"snapStartTime"`
	SnapEndTime    time.Time `json:"snapEndTime"`
	DeletionTime   time.Time `json:"deletionTime"`
	ValidationData []byte    `json:"validationData"`
}

// GetSnapshotMetadata gets the metadata associated with the given snapshot ID.
func (chk *Checker) GetSnapshotMetadata(snapID string) (*SnapshotMetadata, error) {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	return chk.loadSnapshotMetadata(snapID)
}

// GetLiveSnapIDs gets the list of snapshot IDs being tracked by the checker's snapshot store
// that do not have a deletion time associated with them.
func (chk *Checker) GetLiveSnapIDs() []string {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	return chk.SnapIDIndex.GetKeys(liveSnapshotsIdxName)
}

// VerifySnapshotMetadata compares the list of live snapshot IDs present in
// the Checker's metadata against a list of live snapshot IDs in the connected
// repository. This should not be called concurrently, as there is no thread
// safety guaranteed.
func (chk *Checker) VerifySnapshotMetadata() error {
	// Get live snapshot metadata keys
	liveSnapsInMetadata := chk.GetLiveSnapIDs()

	// Get live snapshots listed in the repo itself
	liveSnapsInRepo, err := chk.snapshotIssuer.ListSnapshots()
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
				chk.snapshotMetadataStore.Delete(metaSnapID)
				chk.SnapIDIndex.RemoveFromIndex(metaSnapID, liveSnapshotsIdxName)
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

			err = chk.snapshotIssuer.DeleteSnapshot(liveSnapID, map[string]string{})
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
	b, err := chk.validator.Gather(ctx, sourceDir, opts)
	if err != nil {
		return "", err
	}

	ssStart := clock.Now()

	snapID, err = chk.snapshotIssuer.CreateSnapshot(sourceDir, opts)
	if err != nil {
		return snapID, err
	}

	ssEnd := clock.Now()

	ssMeta := &SnapshotMetadata{
		SnapID:         snapID,
		SnapStartTime:  ssStart,
		SnapEndTime:    ssEnd,
		ValidationData: b,
	}

	chk.mu.Lock()
	defer chk.mu.Unlock()

	err = chk.saveSnapshotMetadata(ssMeta)
	if err != nil {
		return snapID, err
	}

	chk.SnapIDIndex.AddToIndex(snapID, allSnapshotsIdxName)
	chk.SnapIDIndex.AddToIndex(snapID, liveSnapshotsIdxName)

	return snapID, nil
}

// RestoreSnapshot restores a snapshot to the Checker's temporary restore directory
// using the Checker's Snapshotter, and performs a data consistency check on the
// resulting tree using the saved snapshot data.
func (chk *Checker) RestoreSnapshot(ctx context.Context, snapID string, reportOut io.Writer, opts map[string]string) error {
	// Make an independent directory for the restore
	restoreSubDir, err := ioutil.TempDir(chk.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID))
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
	ssMeta, err := chk.safeRestorePrepare(snapID)
	if err != nil {
		return err
	}

	return chk.RestoreVerifySnapshot(ctx, snapID, destPath, ssMeta, reportOut, opts)
}

// safeRestorePrepare wraps key indexing operations that need to be
// executed together under one mutex lock.
func (chk *Checker) safeRestorePrepare(snapID string) (*SnapshotMetadata, error) {
	chk.mu.RLock()
	defer chk.mu.RUnlock()

	if !chk.SnapIDIndex.IsKeyInIndex(snapID, liveSnapshotsIdxName) {
		// Preventing restore of a snapshot ID that has been (or is currently
		// being) deleted.
		log.Printf("Snapshot ID %s is flagged for deletion", snapID)
		return nil, robustness.ErrNoOp
	}

	ssMeta, err := chk.loadSnapshotMetadata(snapID)
	if err != nil {
		return nil, err
	}

	return ssMeta, nil
}

// RestoreVerifySnapshot restores a snapshot and verifies its integrity against
// the metadata provided.
func (chk *Checker) RestoreVerifySnapshot(ctx context.Context, snapID, destPath string, ssMeta *SnapshotMetadata, reportOut io.Writer, opts map[string]string) error {
	err := chk.snapshotIssuer.RestoreSnapshot(snapID, destPath, opts)
	if err != nil {
		return err
	}

	if ssMeta == nil && chk.RecoveryMode {
		var b []byte

		b, err = chk.validator.Gather(ctx, destPath, opts)
		if err != nil {
			return err
		}

		ssMeta = &SnapshotMetadata{
			SnapID:         snapID,
			ValidationData: b,
		}

		chk.mu.Lock()
		defer chk.mu.Unlock()

		err = chk.saveSnapshotMetadata(ssMeta)
		if err != nil {
			return err
		}

		chk.SnapIDIndex.AddToIndex(snapID, allSnapshotsIdxName)
		chk.SnapIDIndex.AddToIndex(snapID, liveSnapshotsIdxName)

		return nil
	}

	err = chk.validator.Compare(ctx, destPath, ssMeta.ValidationData, reportOut, opts)
	if err != nil {
		return err
	}

	return nil
}

const (
	deletedSnapshotsIdxName = "deleted-snapshots-idx"
	liveSnapshotsIdxName    = "live-snapshots-idx"
	allSnapshotsIdxName     = "all-snapshots-idx"
)

// DeleteSnapshot performs the Snapshotter's DeleteSnapshot action, and
// marks the snapshot with the given snapshot ID as deleted.
func (chk *Checker) DeleteSnapshot(ctx context.Context, snapID string, opts map[string]string) error {
	// Load the metadata, ensure the snapshot hasn't already been deleted.
	// If not, remove it from the list of live snapshots. These two tasks must
	// be done atomically.
	ssMeta, err := chk.safeDeletePrepare(snapID)
	if err != nil {
		return err
	}

	err = chk.snapshotIssuer.DeleteSnapshot(snapID, opts)
	if err != nil {
		return err
	}

	ssMeta.DeletionTime = clock.Now()
	ssMeta.ValidationData = nil

	return chk.safeDeleteFinish(ssMeta)
}

// safeDeletePrepare is a routine that will remove the snapshot ID from
// the live snapshots index if the snapshot has not already been deleted.
// The check for deletion must be done atomically with the removal from the
// index to ensure no races, which could lead to false failures if the Delete
// operation runs against a snapshot that has already been deleted.
func (chk *Checker) safeDeletePrepare(snapID string) (*SnapshotMetadata, error) {
	chk.mu.Lock()
	defer chk.mu.Unlock()

	if !chk.SnapIDIndex.IsKeyInIndex(snapID, liveSnapshotsIdxName) {
		// Preventing restore of a snapshot ID that has been already deleted.
		log.Printf("Cannot delete snapshot ID %s as it is not a live snapshot", snapID)
		return nil, robustness.ErrNoOp
	}

	ssMeta, err := chk.loadSnapshotMetadata(snapID)
	if err != nil {
		return nil, err
	}

	// Remove the snapshot ID from the live snapshots index - no concurrent
	// calls to find a live snapshot ID will find this snapshot ID.
	// Place the snapshot ID in the list of IDs with unknown state. If we
	// hit an error during delete, we don't know whether to expect it to
	// still be valid or not.
	chk.SnapIDIndex.RemoveFromIndex(snapID, liveSnapshotsIdxName)

	return ssMeta, nil
}

// safeDelete finish will atomically update the metadata repository with the
// deleted snapshot metadata and add the ID to the deleted snapshots index.
func (chk *Checker) safeDeleteFinish(ssMeta *SnapshotMetadata) error {
	chk.mu.Lock()
	defer chk.mu.Unlock()

	chk.SnapIDIndex.AddToIndex(ssMeta.SnapID, deletedSnapshotsIdxName)

	err := chk.saveSnapshotMetadata(ssMeta)
	if err != nil {
		return err
	}

	return nil
}

func (chk *Checker) saveSnapshotMetadata(ssMeta *SnapshotMetadata) error {
	ssMetaRaw, err := json.Marshal(ssMeta)
	if err != nil {
		return err
	}

	err = chk.snapshotMetadataStore.Store(ssMeta.SnapID, ssMetaRaw)
	if err != nil {
		return err
	}

	return nil
}

func (chk *Checker) loadSnapshotMetadata(snapID string) (*SnapshotMetadata, error) {
	// Lookup metadata by snapshot ID
	b, err := chk.snapshotMetadataStore.Load(snapID)
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
