// Package snapshot manages metadata about snapshots stored in repository.
package snapshot

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// ManifestType is the value of the "type" label for snapshot manifests.
const ManifestType = "snapshot"

// Manifest labels identifying snapshots.
const (
	UsernameLabel = "username"
	HostnameLabel = "hostname"
	PathLabel     = "path"
)

// ErrSnapshotNotFound is returned when a snapshot is not found.
var ErrSnapshotNotFound = errors.New("snapshot not found")

const (
	typeKey = manifest.TypeLabelKey

	loadSnapshotsConcurrency = 50 // number of snapshots to load in parallel
)

var log = logging.Module("kopia/snapshot")

// ListSources lists all snapshot sources in a given repository.
func ListSources(ctx context.Context, rep repo.Repository) ([]SourceInfo, error) {
	items, err := rep.FindManifests(ctx, map[string]string{
		typeKey: ManifestType,
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	uniq := map[SourceInfo]bool{}
	for _, it := range items {
		uniq[sourceInfoFromLabels(it.Labels)] = true
	}

	var infos []SourceInfo
	for k := range uniq {
		infos = append(infos, k)
	}

	return infos, nil
}

func sourceInfoFromLabels(labels map[string]string) SourceInfo {
	return SourceInfo{Host: labels[HostnameLabel], UserName: labels[UsernameLabel], Path: labels[PathLabel]}
}

func sourceInfoToLabels(si SourceInfo) map[string]string {
	m := map[string]string{
		typeKey:       ManifestType,
		HostnameLabel: si.Host,
	}

	if si.UserName != "" {
		m[UsernameLabel] = si.UserName
	}

	if si.Path != "" {
		m[PathLabel] = si.Path
	}

	return m
}

// ListSnapshots lists all snapshots for a given source.
func ListSnapshots(ctx context.Context, rep repo.Repository, si SourceInfo) ([]*Manifest, error) {
	entries, err := rep.FindManifests(ctx, sourceInfoToLabels(si))
	if err != nil {
		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	return LoadSnapshots(ctx, rep, entryIDs(entries))
}

// LoadSnapshot loads and parses a snapshot with a given ID.
func LoadSnapshot(ctx context.Context, rep repo.Repository, manifestID manifest.ID) (*Manifest, error) {
	sm := &Manifest{}

	em, err := rep.GetManifest(ctx, manifestID, sm)
	if err != nil {
		if errors.Is(err, manifest.ErrNotFound) {
			return nil, ErrSnapshotNotFound
		}

		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	if em.Labels[manifest.TypeLabelKey] != ManifestType {
		return nil, errors.New("manifest is not a snapshot")
	}

	sm.ID = manifestID

	return sm, nil
}

// SaveSnapshot persists given snapshot manifest and returns manifest ID.
func SaveSnapshot(ctx context.Context, rep repo.RepositoryWriter, man *Manifest) (manifest.ID, error) {
	if man.Source.Host == "" {
		return "", errors.New("missing host")
	}

	if man.Source.UserName == "" {
		return "", errors.New("missing username")
	}

	if man.Source.Path == "" {
		return "", errors.New("missing path")
	}

	// clear manifest ID in case it was set, since we'll be generating a new one and we don't want
	// to write previous ID in JSON.
	man.ID = ""

	labels := sourceInfoToLabels(man.Source)

	for key, value := range man.Tags {
		if _, ok := labels[key]; ok {
			return "", errors.Errorf("Invalid or duplicate tag <key> found in snapshot. (%s)", key)
		}

		labels[key] = value
	}

	id, err := rep.PutManifest(ctx, labels, man)
	if err != nil {
		return "", errors.Wrap(err, "error putting manifest")
	}

	man.ID = id

	return id, nil
}

// LoadSnapshots efficiently loads and parses a given list of snapshot IDs.
func LoadSnapshots(ctx context.Context, rep repo.Repository, manifestIDs []manifest.ID) ([]*Manifest, error) {
	result := make([]*Manifest, len(manifestIDs))
	sem := make(chan bool, loadSnapshotsConcurrency)

	for i, n := range manifestIDs {
		sem <- true

		go func(i int, n manifest.ID) {
			defer func() { <-sem }()

			m, err := LoadSnapshot(ctx, rep, n)
			if err != nil {
				log(ctx).Errorf("unable to parse snapshot manifest %v: %v", n, err)
				return
			}

			result[i] = m
		}(i, n)
	}

	for range cap(sem) {
		sem <- true
	}

	close(sem)

	successful := result[:0]

	for _, m := range result {
		if m != nil {
			successful = append(successful, m)
		}
	}

	return successful, nil
}

// ListSnapshotManifests returns the list of snapshot manifests for a given source or all sources if nil.
func ListSnapshotManifests(ctx context.Context, rep repo.Repository, src *SourceInfo, tags map[string]string) ([]manifest.ID, error) {
	labels := map[string]string{
		typeKey: ManifestType,
	}

	if src != nil {
		labels = sourceInfoToLabels(*src)
	}

	for key, value := range tags {
		labels[key] = value
	}

	entries, err := rep.FindManifests(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find snapshot manifests")
	}

	return entryIDs(entries), nil
}

// FindSnapshotsByRootObjectID returns the list of matching snapshots for a given rootID.
func FindSnapshotsByRootObjectID(ctx context.Context, rep repo.Repository, rootID object.ID) ([]*Manifest, error) {
	ids, err := ListSnapshotManifests(ctx, rep, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error listing snapshot manifests")
	}

	manifests, err := LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return nil, errors.Wrap(err, "error loading snapshot manifests")
	}

	var result []*Manifest

	for _, m := range manifests {
		if m.RootObjectID() == rootID {
			result = append(result, m)
		}
	}

	return result, nil
}

// UpdateSnapshot updates the snapshot by saving the provided data and deleting old manifest ID.
func UpdateSnapshot(ctx context.Context, rep repo.RepositoryWriter, m *Manifest) error {
	oldID := m.ID

	newID, err := SaveSnapshot(ctx, rep, m)
	if err != nil {
		return errors.Wrap(err, "error saving snapshot")
	}

	if oldID != newID {
		if err := rep.DeleteManifest(ctx, oldID); err != nil {
			return errors.Wrap(err, "error deleting old manifest")
		}
	}

	return nil
}

func entryIDs(entries []*manifest.EntryMetadata) []manifest.ID {
	var ids []manifest.ID
	for _, e := range entries {
		ids = append(ids, e.ID)
	}

	return ids
}
