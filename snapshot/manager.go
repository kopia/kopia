package snapshot

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"

	"errors"

	"github.com/kopia/kopia"
	"github.com/kopia/kopia/vault"
)

const sourcePrefix = "S"
const backupPrefix = "B"
const policyPrefix = "P"

// ErrPolicyNotFound is returned when the policy is not found.
var ErrPolicyNotFound = errors.New("policy not found")

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

// SaveSnapshot persists given snapshot manifest and returns manifest ID.
func (m *Manager) SaveSnapshot(manifest *Manifest) (string, error) {
	uniqueID := make([]byte, 8)
	rand.Read(uniqueID)
	ts := math.MaxInt64 - manifest.StartTime.UnixNano()
	manifestID := fmt.Sprintf("%v%v.%08x.%x", backupPrefix, manifest.Source.HashString(), ts, uniqueID)

	b, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("cannot marshal backup manifest to JSON: %v", err)
	}

	if err := m.vault.Put(manifestID, b); err != nil {
		return "", err
	}

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

// GetPolicy loads snapshot policy for a given source, optionally fall back to default.
func (m *Manager) GetPolicy(src *SourceInfo, fallback bool) (*Policy, error) {
	if p, err := m.getRawPolicy(src); err != ErrPolicyNotFound {
		return p, err
	}

	if !fallback {
		return nil, ErrPolicyNotFound
	}

	if src.Path != "" {
		userHostDefault := *src
		userHostDefault.Path = ""

		if p, err := m.getRawPolicy(&userHostDefault); err != ErrPolicyNotFound {
			return p, nil
		}
	}

	return m.getRawPolicy(&SourceInfo{"", "", ""})
}

// SavePolicy persists the given snapshot policy.
func (m *Manager) SavePolicy(p *Policy) error {
	itemID := fmt.Sprintf("%v%v", policyPrefix, p.Source.HashString())

	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("cannot marshal policy to JSON: %v", err)
	}

	return m.vault.Put(itemID, b)
}

func (m *Manager) getRawPolicy(src *SourceInfo) (*Policy, error) {
	itemID := fmt.Sprintf("%v%v", policyPrefix, src.HashString())

	return m.getPolicyItem(itemID)
}

func (m *Manager) getPolicyItem(itemID string) (*Policy, error) {
	b, err := m.vault.Get(itemID)
	if err == vault.ErrItemNotFound {
		return nil, ErrPolicyNotFound
	}

	if err != nil {
		return nil, err
	}

	var s Policy
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("invalid policy: %v", err)
	}

	return &s, nil
}

// ListPolicies returns a list of all policies stored in a vault.
func (m *Manager) ListPolicies() ([]*Policy, error) {
	names, err := m.vault.List(policyPrefix, -1)
	if err != nil {
		return nil, err
	}

	result := make([]*Policy, len(names))
	sem := make(chan bool, 50)

	for i, n := range names {
		sem <- true
		go func(i int, n string) {
			defer func() { <-sem }()

			p, err := m.getPolicyItem(n)
			if err != nil {
				log.Printf("WARNING: Unable to parse policy %v: %v", n, err)
				return
			}
			result[i] = p
		}(i, n)
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	close(sem)

	return result, nil
}

// NewManager creates new snapshot manager for a given connection.
func NewManager(conn *kopia.Connection) *Manager {
	return &Manager{conn.Vault}
}
