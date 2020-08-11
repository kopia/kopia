// Package snapshot manages metadata about snapshots stored in repository.
package snapshot

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
)

// ManifestType is the value of the "type" label for snapshot manifests.
const ManifestType = "snapshot"

const (
	typeKey = manifest.TypeLabelKey

	loadSnapshotsConcurrency = 50 // number of snapshots to load in parallel
)

var log = logging.GetContextLoggerFunc("kopia/snapshot")

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
	return SourceInfo{Host: labels["hostname"], UserName: labels["username"], Path: labels["path"]}
}

func sourceInfoToLabels(si SourceInfo) map[string]string {
	return map[string]string{
		typeKey:    ManifestType,
		"hostname": si.Host,
		"username": si.UserName,
		"path":     si.Path,
	}
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
		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	if em.Labels[manifest.TypeLabelKey] != ManifestType {
		return nil, errors.Errorf("manifest is not a snapshot")
	}

	sm.ID = manifestID

	return sm, nil
}

// SaveSnapshot persists given snapshot manifest and returns manifest ID.
func SaveSnapshot(ctx context.Context, rep repo.Repository, man *Manifest) (manifest.ID, error) {
	if man.Source.Host == "" {
		return "", errors.New("missing host")
	}

	if man.Source.UserName == "" {
		return "", errors.New("missing username")
	}

	if man.Source.Path == "" {
		return "", errors.New("missing path")
	}

	id, err := rep.PutManifest(ctx, sourceInfoToLabels(man.Source), man)
	if err != nil {
		return "", err
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
				log(ctx).Warningf("unable to parse snapshot manifest %v: %v", n, err)
				return
			}

			result[i] = m
		}(i, n)
	}

	for i := 0; i < cap(sem); i++ {
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
func ListSnapshotManifests(ctx context.Context, rep repo.Repository, src *SourceInfo) ([]manifest.ID, error) {
	labels := map[string]string{
		typeKey: ManifestType,
	}

	if src != nil {
		labels = sourceInfoToLabels(*src)
	}

	entries, err := rep.FindManifests(ctx, labels)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	return entryIDs(entries), nil
}

func entryIDs(entries []*manifest.EntryMetadata) []manifest.ID {
	var ids []manifest.ID
	for _, e := range entries {
		ids = append(ids, e.ID)
	}

	return ids
}
