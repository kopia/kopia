// Package snapshot manages metadata about snapshots stored in repository.
package snapshot

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

var log = kopialogging.Logger("kopia/snapshot")

// ListSources lists all snapshot sources in a given repository.
func ListSources(ctx context.Context, rep *repo.Repository) ([]SourceInfo, error) {
	items, err := rep.Manifests.Find(ctx, map[string]string{
		"type": "snapshot",
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
		"type":     "snapshot",
		"hostname": si.Host,
		"username": si.UserName,
		"path":     si.Path,
	}
}

// ListSnapshots lists all snapshots for a given source.
func ListSnapshots(ctx context.Context, rep *repo.Repository, si SourceInfo) ([]*Manifest, error) {
	entries, err := rep.Manifests.Find(ctx, sourceInfoToLabels(si))
	if err != nil {
		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	return LoadSnapshots(ctx, rep, entryIDs(entries))
}

// LoadSnapshot loads and parses a snapshot with a given ID.
func LoadSnapshot(ctx context.Context, rep *repo.Repository, manifestID manifest.ID) (*Manifest, error) {
	sm := &Manifest{}
	if err := rep.Manifests.Get(ctx, manifestID, sm); err != nil {
		return nil, errors.Wrap(err, "unable to find manifest entries")
	}

	sm.ID = manifestID

	return sm, nil
}

// SaveSnapshot persists given snapshot manifest and returns manifest ID.
func SaveSnapshot(ctx context.Context, rep *repo.Repository, man *Manifest) (manifest.ID, error) {
	if man.Source.Host == "" {
		return "", errors.New("missing host")
	}

	if man.Source.UserName == "" {
		return "", errors.New("missing username")
	}

	if man.Source.Path == "" {
		return "", errors.New("missing path")
	}

	id, err := rep.Manifests.Put(ctx, sourceInfoToLabels(man.Source), man)
	if err != nil {
		return "", err
	}

	man.ID = id

	return id, nil
}

// LoadSnapshots efficiently loads and parses a given list of snapshot IDs.
func LoadSnapshots(ctx context.Context, rep *repo.Repository, manifestIDs []manifest.ID) ([]*Manifest, error) {
	result := make([]*Manifest, len(manifestIDs))
	sem := make(chan bool, 50)

	for i, n := range manifestIDs {
		sem <- true

		go func(i int, n manifest.ID) {
			defer func() { <-sem }()

			m, err := LoadSnapshot(ctx, rep, n)
			if err != nil {
				log.Warningf("unable to parse snapshot manifest %v: %v", n, err)
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
func ListSnapshotManifests(ctx context.Context, rep *repo.Repository, src *SourceInfo) ([]manifest.ID, error) {
	labels := map[string]string{
		"type": "snapshot",
	}

	if src != nil {
		labels = sourceInfoToLabels(*src)
	}

	entries, err := rep.Manifests.Find(ctx, labels)
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
