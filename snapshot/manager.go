package snapshot

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/kopia/kopia/repo"
)

const snapshotPrefix = "S"

// GlobalPolicySourceInfo is a source where global policy is attached.
var GlobalPolicySourceInfo = &SourceInfo{}

// Manager manages filesystem snapshots.
type Manager struct {
	repository       *repo.Repository
	snapshotIDSecret []byte
}

// ListSources lists all snapshot sources.
func (m *Manager) ListSources() ([]*SourceInfo, error) {
	names, err := m.repository.Metadata.List(snapshotPrefix)
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
func (m *Manager) ListSnapshots(si *SourceInfo) ([]*Manifest, error) {
	names, err := m.repository.Metadata.List(m.snapshotIDPrefix(si))
	if err != nil {
		return nil, err
	}

	return m.LoadSnapshots(names)
}

// LoadSnapshot loads and parses a snapshot with a given ID.
func (m *Manager) LoadSnapshot(manifestID string) (*Manifest, error) {
	b, err := m.repository.Metadata.GetMetadata(manifestID)
	if err != nil {
		return nil, fmt.Errorf("error loading previous backup: %v", err)
	}

	var s Manifest
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("invalid previous backup manifest: %v", err)
	}

	return &s, nil
}

// SaveSnapshot persists given snapshot manifest and returns manifest ID.
func (m *Manager) SaveSnapshot(manifest *Manifest) (string, error) {
	uniqueID := make([]byte, 8)
	rand.Read(uniqueID)
	ts := math.MaxInt64 - manifest.StartTime.UnixNano()
	manifestID := fmt.Sprintf("%v.%08x.%x", m.snapshotIDPrefix(&manifest.Source), ts, uniqueID)

	b, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("cannot marshal backup manifest to JSON: %v", err)
	}

	if err := m.repository.Metadata.Put(manifestID, b); err != nil {
		return "", err
	}

	m.repository.Manifests.Add(map[string]string{
		"type":     "snapshot",
		"hostname": manifest.Source.Host,
		"username": manifest.Source.UserName,
		"path":     manifest.Source.Path,
	}, manifest)

	return "", nil
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

	successful := result[:0]
	for _, m := range result {
		if m != nil {
			successful = append(successful, m)
		}
	}

	return successful, nil
}

// ListSnapshotManifests returns the list of snapshot manifests for a given source or all sources if nil.
func (m *Manager) ListSnapshotManifests(src *SourceInfo) ([]string, error) {
	prefix := snapshotPrefix
	if src != nil {
		prefix = m.snapshotIDPrefix(src)
	}

	return m.repository.Metadata.List(prefix)
}

func cloneSourceInfo(si SourceInfo) *SourceInfo {
	return &si
}

func (m *Manager) snapshotIDPrefix(src *SourceInfo) string {
	return fmt.Sprintf("%v%v", snapshotPrefix, src.HashString(m.snapshotIDSecret))
}

// NewManager creates new snapshot manager for a given connection.
func NewManager(r *repo.Repository) *Manager {
	return &Manager{
		r,
		r.KeyManager.DeriveKey([]byte("snapshot-id"), 32),
	}
}
