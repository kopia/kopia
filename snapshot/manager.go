package snapshot

import (
	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/manifest"
	"github.com/kopia/kopia/repo"
)

var log = kopialogging.Logger("kopia/snapshot")

// GlobalPolicySourceInfo is a source where global policy is attached.
var GlobalPolicySourceInfo = SourceInfo{}

// Manager manages filesystem snapshots.
type Manager struct {
	Repository *repo.Repository
}

// ListSources lists all snapshot sources.
func (m *Manager) ListSources() []SourceInfo {
	items := m.Repository.Manifests.Find(map[string]string{
		"type": "snapshot",
	})

	uniq := map[SourceInfo]bool{}
	for _, it := range items {
		uniq[sourceInfoFromLabels(it.Labels)] = true
	}

	var infos []SourceInfo
	for k := range uniq {
		infos = append(infos, k)
	}

	return infos
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
func (m *Manager) ListSnapshots(si SourceInfo) ([]*Manifest, error) {
	return m.LoadSnapshots(manifest.EntryIDs(m.Repository.Manifests.Find(sourceInfoToLabels(si))))
}

// LoadSnapshot loads and parses a snapshot with a given ID.
func (m *Manager) LoadSnapshot(manifestID string) (*Manifest, error) {
	sm := &Manifest{}
	if err := m.Repository.Manifests.Get(manifestID, sm); err != nil {
		return nil, err
	}

	sm.ID = manifestID

	return sm, nil
}

// SaveSnapshot persists given snapshot manifest and returns manifest ID.
func (m *Manager) SaveSnapshot(manifest *Manifest) (string, error) {
	return m.Repository.Manifests.Put(sourceInfoToLabels(manifest.Source), manifest)
}

// LoadSnapshots efficiently loads and parses a given list of snapshot IDs.
func (m *Manager) LoadSnapshots(names []string) ([]*Manifest, error) {
	result := make([]*Manifest, len(names))
	sem := make(chan bool, 50)

	for i, n := range names {
		sem <- true
		go func(i int, n string) {
			defer func() { <-sem }()

			m, err := m.LoadSnapshot(n)
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
func (m *Manager) ListSnapshotManifests(src *SourceInfo) []string {
	labels := map[string]string{
		"type": "snapshot",
	}

	if src != nil {
		labels = sourceInfoToLabels(*src)
	}

	return manifest.EntryIDs(m.Repository.Manifests.Find(labels))
}

// NewManager creates new snapshot manager for a given connection.
func NewManager(r *repo.Repository) *Manager {
	return &Manager{
		r,
	}
}
