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
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness/snap"
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
	snapshotIssuer        snap.Snapshotter
	snapshotMetadataStore snapmeta.Store
	validator             Comparer
	RecoveryMode          bool
	DeleteLimit           int
}

// NewChecker instantiates a new Checker, returning its pointer. A temporary
// directory is created to mount restored data.
func NewChecker(snapIssuer snap.Snapshotter, snapmetaStore snapmeta.Store, validator Comparer, restoreDir string) (*Checker, error) {
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
	return chk.snapshotMetadataStore.GetKeys(allSnapshotsIdxName)
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
	return chk.loadSnapshotMetadata(snapID)
}

// GetLiveSnapIDs gets the list of snapshot IDs being tracked by the checker's snapshot store
// that do not have a deletion time associated with them.
func (chk *Checker) GetLiveSnapIDs() []string {
	return chk.snapshotMetadataStore.GetKeys(liveSnapshotsIdxName)
}

// IsSnapshotIDDeleted reports whether the metadata associated with the provided snapshot ID
// has it marked as deleted.
func (chk *Checker) IsSnapshotIDDeleted(snapID string) (bool, error) {
	md, err := chk.loadSnapshotMetadata(snapID)
	if err != nil {
		return false, err
	}

	return !md.DeletionTime.IsZero(), nil
}

// VerifySnapshotMetadata compares the list of live snapshot IDs present in
// the Checker's metadata against a list of live snapshot IDs in the connected
// repository.
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
				chk.snapshotMetadataStore.Delete(metaSnapID)
				chk.snapshotMetadataStore.RemoveFromIndex(metaSnapID, liveSnapshotsIdxName)
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

			err = chk.snapshotIssuer.DeleteSnapshot(liveSnapID)
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
func (chk *Checker) TakeSnapshot(ctx context.Context, sourceDir string) (snapID string, err error) {
	b, err := chk.validator.Gather(ctx, sourceDir)
	if err != nil {
		return "", err
	}

	ssStart := clock.Now()

	snapID, err = chk.snapshotIssuer.CreateSnapshot(sourceDir)
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

	err = chk.saveSnapshotMetadata(ssMeta)
	if err != nil {
		return snapID, err
	}

	chk.snapshotMetadataStore.AddToIndex(snapID, allSnapshotsIdxName)
	chk.snapshotMetadataStore.AddToIndex(snapID, liveSnapshotsIdxName)

	return snapID, nil
}

// RestoreSnapshot restores a snapshot to the Checker's temporary restore directory
// using the Checker's Snapshotter, and performs a data consistency check on the
// resulting tree using the saved snapshot data.
func (chk *Checker) RestoreSnapshot(ctx context.Context, snapID string, reportOut io.Writer) error {
	// Make an independent directory for the restore
	restoreSubDir, err := ioutil.TempDir(chk.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID))
	if err != nil {
		return err
	}

	defer os.RemoveAll(restoreSubDir) //nolint:errcheck

	return chk.RestoreSnapshotToPath(ctx, snapID, restoreSubDir, reportOut)
}

// RestoreSnapshotToPath restores a snapshot to the requested path
// using the Checker's Snapshotter, and performs a data consistency check on the
// resulting tree using the saved snapshot data.
func (chk *Checker) RestoreSnapshotToPath(ctx context.Context, snapID, destPath string, reportOut io.Writer) error {
	ssMeta, err := chk.loadSnapshotMetadata(snapID)
	if err != nil {
		return err
	}

	return chk.RestoreVerifySnapshot(ctx, snapID, destPath, ssMeta, reportOut)
}

// RestoreVerifySnapshot restores a snapshot and verifies its integrity against
// the metadata provided.
func (chk *Checker) RestoreVerifySnapshot(ctx context.Context, snapID, destPath string, ssMeta *SnapshotMetadata, reportOut io.Writer) error {
	err := chk.snapshotIssuer.RestoreSnapshot(snapID, destPath)
	if err != nil {
		return err
	}

	if ssMeta == nil && chk.RecoveryMode {
		var b []byte

		b, err = chk.validator.Gather(ctx, destPath)
		if err != nil {
			return err
		}

		ssMeta = &SnapshotMetadata{
			SnapID:         snapID,
			ValidationData: b,
		}

		return chk.saveSnapshotMetadata(ssMeta)
	}

	err = chk.validator.Compare(ctx, destPath, ssMeta.ValidationData, reportOut)
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
func (chk *Checker) DeleteSnapshot(ctx context.Context, snapID string) error {
	err := chk.snapshotIssuer.DeleteSnapshot(snapID)
	if err != nil {
		return err
	}

	ssMeta, err := chk.loadSnapshotMetadata(snapID)
	if err != nil {
		return err
	}

	ssMeta.DeletionTime = time.Now()
	ssMeta.ValidationData = nil

	err = chk.saveSnapshotMetadata(ssMeta)
	if err != nil {
		return err
	}

	chk.snapshotMetadataStore.AddToIndex(ssMeta.SnapID, deletedSnapshotsIdxName)
	chk.snapshotMetadataStore.RemoveFromIndex(ssMeta.SnapID, liveSnapshotsIdxName)

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
