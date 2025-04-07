package snapshotfs

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// ParseObjectIDWithPath interprets the given ID string (which could be an object ID optionally followed by
// nested path specification) and returns corresponding object.ID.
func ParseObjectIDWithPath(ctx context.Context, rep repo.Repository, objectIDWithPath string) (object.ID, error) {
	parts := strings.Split(objectIDWithPath, "/")

	oid, err := object.ParseID(parts[0])
	if err != nil {
		return object.EmptyID, errors.Wrapf(err, "can't parse object ID %v", objectIDWithPath)
	}

	if len(parts) == 1 {
		return oid, nil
	}

	return parseNestedObjectID(ctx, AutoDetectEntryFromObjectID(ctx, rep, oid, ""), parts[1:])
}

// GetNestedEntry returns nested entry with a given name path.
func GetNestedEntry(ctx context.Context, startingDir fs.Entry, pathElements []string) (fs.Entry, error) {
	current := startingDir

	for _, part := range pathElements {
		if part == "" {
			continue
		}

		dir, ok := current.(fs.Directory)
		if !ok {
			return nil, errors.Errorf("entry not found %q: parent is not a directory", part)
		}

		e, err := dir.Child(ctx, part)
		if err != nil {
			return nil, errors.Wrap(err, "error reading directory")
		}

		if e == nil {
			return nil, errors.Errorf("entry not found: %q", part)
		}

		current = e
	}

	return current, nil
}

func parseNestedObjectID(ctx context.Context, startingDir fs.Entry, parts []string) (object.ID, error) {
	e, err := GetNestedEntry(ctx, startingDir, parts)
	if err != nil {
		return object.EmptyID, err
	}

	hoid, ok := e.(object.HasObjectID)
	if !ok {
		return object.EmptyID, errors.New("entry without ObjectID")
	}

	return hoid.ObjectID(), nil
}

// findSnapshotByRootObjectIDOrManifestID returns the list of matching snapshots for a given rootID.
// which can be either snapshot manifst ID (which matches 0 or 1 snapshots)
// or the root object ID (which can match arbitrary number of snapshots).
// If multiple snapshots match and they don't agree on root object attributes and consistentAttributes==true
// the function fails, otherwise it returns the latest of the snapshots.
func findSnapshotByRootObjectIDOrManifestID(ctx context.Context, rep repo.Repository, rootID string, consistentAttributes bool) (*snapshot.Manifest, error) {
	m, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(rootID))
	if err == nil {
		return m, nil
	}

	rootOID, err := object.ParseID(rootID)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing root object ID")
	}

	mans, err := snapshot.FindSnapshotsByRootObjectID(ctx, rep, rootOID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to find snapshots by ID %v", rootID)
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
	if consistentAttributes {
		return nil, errors.Errorf("found multiple snapshots matching %v with inconsistent root attributes", rootID)
	}

	repoFSLog(ctx).Debugf("Found multiple snapshots matching %v with inconsistent root attributes. Picking latest one.", rootID)

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

// FilesystemEntryFromIDWithPath returns a filesystem entry for the provided object ID, which
// can be a snapshot manifest ID or an object ID with path.
// If multiple snapshots match and they don't agree on root object attributes and consistentAttributes==true
// the function fails, otherwise it returns the latest of the snapshots.
func FilesystemEntryFromIDWithPath(ctx context.Context, rep repo.Repository, rootID string, consistentAttributes bool) (fs.Entry, error) {
	pathElements := strings.Split(filepath.ToSlash(rootID), "/")

	if len(pathElements) > 1 {
		// if a path is provided, consistentAttributes is meaningless since descending into nested path is
		// always unambiguous because parent always has full attributes.
		consistentAttributes = false
	}

	var startingEntry fs.Entry

	man, err := findSnapshotByRootObjectIDOrManifestID(ctx, rep, pathElements[0], consistentAttributes)
	if err != nil {
		return nil, err
	}

	if man != nil {
		// ID was unambiguously resolved to a snapshot, which means we have data about the root directory itself.
		startingEntry, err = SnapshotRoot(rep, man)
		if err != nil {
			return nil, err
		}
	} else {
		oid, err := object.ParseID(pathElements[0])
		if err != nil {
			return nil, errors.Wrapf(err, "can't parse object ID %v", rootID)
		}

		startingEntry = AutoDetectEntryFromObjectID(ctx, rep, oid, "")
	}

	return GetNestedEntry(ctx, startingEntry, pathElements[1:])
}

// FilesystemDirectoryFromIDWithPath returns a filesystem directory entry for the provided object ID, which
// can be a snapshot manifest ID or an object ID with path.
func FilesystemDirectoryFromIDWithPath(ctx context.Context, rep repo.Repository, rootID string, consistentAttributes bool) (fs.Directory, error) {
	e, err := FilesystemEntryFromIDWithPath(ctx, rep, rootID, consistentAttributes)
	if err != nil {
		return nil, err
	}

	if dir, ok := e.(fs.Directory); ok {
		return dir, nil
	}

	return nil, errors.Errorf("%v is not a directory object", rootID)
}

func consistentSnapshotMetadata(m1, m2 *snapshot.Manifest) bool {
	if m1.RootEntry == nil || m2.RootEntry == nil {
		return false
	}

	return toJSON(m1.RootEntry) == toJSON(m2.RootEntry)
}

func toJSON(v *snapshot.DirEntry) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<invalid>"
	}

	return string(b)
}

// GetEntryFromPlaceholder returns a fs.Entry for shallow placeholder
// defp referencing a real Entry in Repository r.
func GetEntryFromPlaceholder(ctx context.Context, r repo.Repository, defp snapshot.HasDirEntryOrNil) (fs.Entry, error) {
	de, err := defp.DirEntryOrNil(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get direntry from placeholder")
	}

	repoFSLog(ctx).Debugf("GetDirEntryFromPlaceholder %v %v ", r, de)

	return EntryFromDirEntry(r, de), nil
}
