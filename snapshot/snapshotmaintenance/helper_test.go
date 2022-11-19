package snapshotmaintenance_test

import (
	"context"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// Create snapshots an FS entry.
func createSnapshot(ctx context.Context, rep repo.RepositoryWriter, e fs.Entry, si snapshot.SourceInfo, description string) (*snapshot.Manifest, error) {
	// sanitize source path
	si.Path = filepath.Clean(si.Path)

	policyTree, err := policy.TreeForSource(ctx, rep, si)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get policy tree")
	}

	previous, err := findPreviousSnapshotManifest(ctx, rep, si)
	if err != nil {
		return nil, err
	}

	u := snapshotfs.NewUploader(rep)

	manifest, err := u.Upload(ctx, e, policyTree, nil, si, previous...)
	if err != nil {
		return nil, err
	}

	manifest.Description = description
	if _, err = snapshot.SaveSnapshot(ctx, rep, manifest); err != nil {
		return nil, errors.Wrap(err, "cannot save manifest")
	}

	return manifest, nil
}

// findPreviousSnapshotManifest returns the list of previous snapshots for a given source, including
// last complete snapshot and possibly some number of incomplete snapshots following it.
// this would belong in the snapshot package.
func findPreviousSnapshotManifest(ctx context.Context, rep repo.Repository, sourceInfo snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, rep, sourceInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error listing previous snapshots")
	}

	// phase 1 - find latest complete snapshot.
	var previousComplete *snapshot.Manifest

	var result []*snapshot.Manifest

	for _, p := range man {
		if p.IncompleteReason == "" && (previousComplete == nil || p.StartTime.After(previousComplete.StartTime)) {
			previousComplete = p
		}
	}

	if previousComplete != nil {
		result = append(result, previousComplete)
	}

	// add all incomplete snapshots after that
	for _, p := range man {
		if p.IncompleteReason != "" && p.StartTime.After(previousComplete.StartTime) {
			result = append(result, p)
		}
	}

	return result, nil
}
