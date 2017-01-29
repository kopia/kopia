package snapshot

import (
	"encoding/json"
	"fmt"
	"log"

	"strings"

	"github.com/kopia/kopia/vault"
)

const sourcePrefix = "S"
const backupPrefix = "B"

// Manager manages filesystem snapshots.
type Manager struct {
	vault *vault.Vault
}

// Sources lists all snapshot sources.
func (m *Manager) Sources() ([]*SourceInfo, error) {
	names, err := m.vault.List(backupPrefix, -1)
	if err != nil {
		return nil, err
	}

	var snapshotIDs []string
	var lastSourceID string

	for _, b := range names {
		sourceID := strings.Split(b, ".")[0]
		if sourceID != lastSourceID {
			snapshotIDs = append(snapshotIDs, b)
			lastSourceID = sourceID
		}
	}

	bs, err := m.LoadSnapshots(snapshotIDs)
	if err != nil {
		return nil, err
	}

	var sourceInfos []*SourceInfo
	for _, b := range bs {
		sourceInfos = append(sourceInfos, &b.Source)
	}

	return sourceInfos, nil
}

// ListSnapshots lists all snapshots for a given source.
func (m *Manager) ListSnapshots(si *SourceInfo, limit int) ([]*Manifest, error) {
	names, err := m.vault.List(backupPrefix+si.HashString(), limit)
	if err != nil {
		return nil, err
	}

	return m.LoadSnapshots(names)
}

// LoadSnapshot loads and parses a snapshot with a given ID.
func (m *Manager) LoadSnapshot(manifestID string) (*Manifest, error) {
	b, err := m.vault.Get(manifestID)
	if err != nil {
		return nil, fmt.Errorf("error loading previous backup: %v", err)
	}

	var s Manifest
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("invalid previous backup manifest: %v", err)
	}

	return &s, nil
}

// SaveSnapshot persists given snapshot manifest with a given ID.
func (m *Manager) SaveSnapshot(manifestID string, manifest *Manifest) error {
	b, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("cannot marshal backup manifest to JSON: %v", err)
	}

	return m.vault.Put(manifestID, b)
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
				log.Printf("WARNING: Unable to parse snapshot manifest %v: %v", n, err)
				return
			}
			result[i] = m
		}(i, n)
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	close(sem)

	return result, nil
}

// ListSnapshotManifests returns the list of snapshot manifests for a given source or all sources if nil.
// Limit specifies how mamy manifests to retrieve (-1 == unlimited).
func (m *Manager) ListSnapshotManifests(src *SourceInfo, limit int) ([]string, error) {
	var prefix string

	if src != nil {
		prefix = src.HashString()
	}

	return m.vault.List(backupPrefix+prefix, limit)
}

// NewManager creates new snapshot manager for a given vault and repository.
func NewManager(vault *vault.Vault) *Manager {
	return &Manager{vault}
}
