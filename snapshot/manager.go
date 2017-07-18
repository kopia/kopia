package snapshot

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"

	"sync"

	"github.com/kopia/kopia/repo"
)

const snapshotPrefix = "S"
const policyPrefix = "P"

// ErrPolicyNotFound is returned when the policy is not found.
var ErrPolicyNotFound = errors.New("policy not found")

// GlobalPolicySourceInfo is a source where global policy is attached.
var GlobalPolicySourceInfo = &SourceInfo{}

// Manager manages filesystem snapshots.
type Manager struct {
	repository       *repo.Repository
	snapshotIDSecret []byte
	policyIDSecret   []byte
}

// ListSources lists all snapshot sources.
func (m *Manager) ListSources() ([]*SourceInfo, error) {
	names, err := m.repository.ListMetadata(snapshotPrefix, -1)
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
	names, err := m.repository.ListMetadata(m.snapshotIDPrefix(si), limit)
	if err != nil {
		return nil, err
	}

	return m.LoadSnapshots(names)
}

// LoadSnapshot loads and parses a snapshot with a given ID.
func (m *Manager) LoadSnapshot(manifestID string) (*Manifest, error) {
	b, err := m.repository.GetMetadata(manifestID)
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

	if err := m.repository.PutMetadata(manifestID, b); err != nil {
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

	successful := result[:0]
	for _, m := range result {
		if m != nil {
			successful = append(successful, m)
		}
	}

	return successful, nil
}

// ListSnapshotManifests returns the list of snapshot manifests for a given source or all sources if nil.
// Limit specifies how mamy manifests to retrieve (-1 == unlimited).
func (m *Manager) ListSnapshotManifests(src *SourceInfo, limit int) ([]string, error) {
	prefix := snapshotPrefix
	if src != nil {
		prefix = m.snapshotIDPrefix(src)
	}

	return m.repository.ListMetadata(prefix, limit)
}

// GetEffectivePolicy calculates effective snapshot policy for a given source by combining the source-specifc policy (if any)
// with parent policies. The source must contain a path.
func (m *Manager) GetEffectivePolicy(src *SourceInfo) (*Policy, error) {
	if src == nil || src.Path == "" || src.Host == "" || src.UserName == "" {
		return nil, errors.New("effective policy can only be computed for paths")
	}

	tmp := *src

	var sources []*SourceInfo

	for len(tmp.Path) > 0 {
		sources = append(sources, cloneSourceInfo(tmp))
		parentPath := filepath.Dir(tmp.Path)
		if parentPath == tmp.Path {
			break
		}
		tmp.Path = parentPath
	}

	// username@host
	tmp.Path = ""
	sources = append(sources, cloneSourceInfo(tmp))

	// @host
	tmp.UserName = ""
	sources = append(sources, cloneSourceInfo(tmp))

	// global
	tmp.Host = ""
	sources = append(sources, cloneSourceInfo(tmp))

	policies := make([]*Policy, len(sources))
	errors := make([]error, len(sources))
	var wg sync.WaitGroup
	wg.Add(len(policies))

	// Read all sources in parallel
	for i := range sources {
		go func(i int) {
			defer wg.Done()

			p, err := m.GetPolicy(sources[i])
			if err == nil {
				policies[i] = p
			} else {
				errors[i] = err
			}
		}(i)
	}
	wg.Wait()

	// If all reads were successful or we got ErrPolicyNotFound, build a list of successful policies.
	var foundPolicies []*Policy
	for i := range sources {
		if errors[i] == nil && policies[i] != nil {
			foundPolicies = append(foundPolicies, policies[i])
		} else if errors[i] != ErrPolicyNotFound {
			return nil, fmt.Errorf("got unexpected error when loading policy for %v: %v", sources[i], errors[i])
		}
	}

	merged := mergePolicies(foundPolicies)
	merged.Source = *src
	return merged, nil
}

func cloneSourceInfo(si SourceInfo) *SourceInfo {
	return &si
}

// SavePolicy persists the given snapshot policy.
func (m *Manager) SavePolicy(p *Policy) error {
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("cannot marshal policy to JSON: %v", err)
	}

	return m.repository.PutMetadata(m.policyID(&p.Source), b)
}

// RemovePolicy removes the policy for a given source
func (m *Manager) RemovePolicy(src *SourceInfo) error {
	return m.repository.RemoveMetadata(m.policyID(src))
}

// GetPolicy retrieves the Policy for a given source, if defined.
// Returns ErrPolicyNotFound if policy not defined.
func (m *Manager) GetPolicy(src *SourceInfo) (*Policy, error) {
	return m.getPolicyItem(m.policyID(src))
}

func (m *Manager) snapshotIDPrefix(src *SourceInfo) string {
	return fmt.Sprintf("%v%v", snapshotPrefix, src.HashString(m.snapshotIDSecret))
}

func (m *Manager) policyID(src *SourceInfo) string {
	return fmt.Sprintf("%v%v", policyPrefix, src.HashString(m.policyIDSecret))
}

func (m *Manager) getPolicyItem(itemID string) (*Policy, error) {
	b, err := m.repository.GetMetadata(itemID)
	if err == repo.ErrMetadataNotFound {
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

// ListPolicies returns a list of all snapshot policies.
func (m *Manager) ListPolicies() ([]*Policy, error) {
	names, err := m.repository.ListMetadata(policyPrefix, -1)
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

	successful := result[:0]
	for _, m := range result {
		if m != nil {
			successful = append(successful, m)
		}
	}

	return successful, nil
}

// NewManager creates new snapshot manager for a given connection.
func NewManager(r *repo.Repository) *Manager {
	return &Manager{
		r,
		r.DeriveKey([]byte("snapshot-id"), 32),
		r.DeriveKey([]byte("policyID-id"), 32),
	}
}
