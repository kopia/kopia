package cli

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// ParseObjectID interprets the given ID string and returns corresponding object.ID.
func parseObjectID(ctx context.Context, rep repo.Repository, id string) (object.ID, error) {
	parts := strings.Split(id, "/")

	oid, err := object.ParseID(parts[0])
	if err != nil {
		return "", errors.Wrapf(err, "can't parse object ID %v", id)
	}

	if len(parts) == 1 {
		return oid, nil
	}

	return parseNestedObjectID(ctx, snapshotfs.AutoDetectEntryFromObjectID(ctx, rep, oid, ""), parts[1:])
}

func getNestedEntry(ctx context.Context, startingDir fs.Entry, parts []string) (fs.Entry, error) {
	current := startingDir

	for _, part := range parts {
		if part == "" {
			continue
		}

		dir, ok := current.(fs.Directory)
		if !ok {
			return nil, errors.Errorf("entry not found %q: parent is not a directory", part)
		}

		entries, err := dir.Readdir(ctx)
		if err != nil {
			return nil, err
		}

		e := entries.FindByName(part)
		if e == nil {
			return nil, errors.Errorf("entry not found: %q", part)
		}

		current = e
	}

	return current, nil
}

func parseNestedObjectID(ctx context.Context, startingDir fs.Entry, parts []string) (object.ID, error) {
	e, err := getNestedEntry(ctx, startingDir, parts)
	if err != nil {
		return "", err
	}

	return e.(object.HasObjectID).ObjectID(), nil
}

func findSnapshotsByRootObjectID(ctx context.Context, rep repo.Repository, rootID object.ID) ([]*snapshot.Manifest, error) {
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error listing snapshot manifests")
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return nil, errors.Wrap(err, "error loading snapshot manifests")
	}

	var result []*snapshot.Manifest

	for _, m := range manifests {
		if m.RootObjectID() == rootID {
			result = append(result, m)
		}
	}

	return result, nil
}

func findSnapshotByRootObjectIDOrManifestID(ctx context.Context, rep repo.Repository, rootID string) (*snapshot.Manifest, error) {
	m, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(rootID))
	if err == nil {
		return m, nil
	}

	mans, err := findSnapshotsByRootObjectID(ctx, rep, object.ID(rootID))
	if err != nil {
		return nil, err
	}

	// no matching snapshots.
	if len(mans) == 0 {
		return nil, nil
	}

	// all snapshots have consistent metadata, pick any.
	if areSnapshotsConsistent(mans) {
		return mans[0], nil
	}

	// at this point we found multiple snapshots with the same root ID which don't agree on other
	// metadata (the attributes, ACLs, ownership, etc. of the root)
	if os.Getenv("KOPIA_REQUIRE_UNAMBIGUOUS_ROOT") != "" {
		return nil, errors.Errorf("found multiple snapshots matching %v with inconsistent root metadata.", rootID)
	}

	log(ctx).Warningf("Found multiple snapshots matching %v with inconsistent root metadata. Picking latest one.", rootID)

	return latestManifest(mans), nil
}

func areSnapshotsConsistent(mans []*snapshot.Manifest) bool {
	for _, m := range mans {
		if !consistentSnapshotMetadata(m, mans[0]) {
			return false
		}
	}

	return true
}

func latestManifest(mans []*snapshot.Manifest) *snapshot.Manifest {
	latest := mans[0]

	for _, m := range mans {
		if m.StartTime.After(latest.StartTime) {
			latest = m
		}
	}

	return latest
}

func filesystemEntryFromID(ctx context.Context, rep repo.Repository, rootID string) (fs.Entry, error) {
	man, err := findSnapshotByRootObjectIDOrManifestID(ctx, rep, rootID)
	if err != nil {
		return nil, err
	}

	if man != nil {
		// ID was unambiguously resolved to a snapshot, which means we have data about the root directory itself.
		return snapshotfs.SnapshotRoot(rep, man)
	}

	parts := strings.Split(rootID, "/")

	oid, err := object.ParseID(parts[0])
	if err != nil {
		return nil, errors.Wrapf(err, "can't parse object ID %v", rootID)
	}

	return getNestedEntry(ctx, snapshotfs.AutoDetectEntryFromObjectID(ctx, rep, oid, ""), parts[1:])
}

func consistentSnapshotMetadata(m1, m2 *snapshot.Manifest) bool {
	if m1.RootEntry == nil || m2.RootEntry == nil {
		return false
	}

	return toJSON(m1.RootEntry) == toJSON(m2.RootEntry)
}

func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
