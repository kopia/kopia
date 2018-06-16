package snapshot

import (
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/manifest"
	"github.com/kopia/kopia/repo"
	"github.com/rs/zerolog/log"
)

// PolicyManager manages snapshotting policies.
type PolicyManager struct {
	repository *repo.Repository
}

// GetEffectivePolicy calculates effective snapshot policy for a given source by combining the source-specifc policy (if any)
// with parent policies. The source must contain a path.
// Returns the effective policies and all source policies that contributed to that (most specific first).
func (m *PolicyManager) GetEffectivePolicy(si SourceInfo) (*Policy, []*Policy, error) {
	var md []*manifest.EntryMetadata

	// Find policies applying to paths all the way up to the root.
	for tmp := si; len(si.Path) > 0; {
		manifests := m.repository.Manifests.Find(labelsForSource(tmp))
		md = append(md, manifests...)

		parentPath := filepath.Dir(tmp.Path)
		if parentPath == tmp.Path {
			break
		}

		tmp.Path = parentPath
	}

	// Try user@host policy
	md = append(md, m.repository.Manifests.Find(labelsForSource(SourceInfo{Host: si.Host, UserName: si.UserName}))...)

	// Try host-level policy.
	md = append(md, m.repository.Manifests.Find(labelsForSource(SourceInfo{Host: si.Host}))...)

	// Global policy.
	globalManifests := m.repository.Manifests.Find(labelsForSource(GlobalPolicySourceInfo))
	md = append(md, globalManifests...)

	var policies []*Policy
	for _, em := range md {
		p := &Policy{}
		if err := m.repository.Manifests.Get(em.ID, &p); err != nil {
			return nil, nil, fmt.Errorf("got unexpected error when loading policy item %v: %v", em.ID, err)
		}
		p.Labels = em.Labels
		policies = append(policies, p)
		log.Printf("loaded parent policy for %v: %v", si, p.Target())
	}

	merged := MergePolicies(policies)
	merged.Labels = labelsForSource(si)

	return merged, policies, nil
}

// GetDefinedPolicy returns the policy defined on the provided SourceInfo or ErrPolicyNotFound if not present.
func (m *PolicyManager) GetDefinedPolicy(si SourceInfo) (*Policy, error) {
	md := m.repository.Manifests.Find(labelsForSource(si))

	if len(md) == 0 {
		return nil, ErrPolicyNotFound
	}

	if len(md) == 1 {
		p := &Policy{}

		err := m.repository.Manifests.Get(md[0].ID, p)
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}

		if err != nil {
			return nil, err
		}

		em, err := m.repository.Manifests.GetMetadata(md[0].ID)
		if err != nil {
			return nil, ErrPolicyNotFound
		}

		p.Labels = em.Labels
		return p, nil
	}

	return nil, fmt.Errorf("ambiguous policy")
}

// SetPolicy sets the policy on a given source.
func (m *PolicyManager) SetPolicy(si SourceInfo, pol *Policy) error {
	md := m.repository.Manifests.Find(labelsForSource(si))

	if _, err := m.repository.Manifests.Put(labelsForSource(si), pol); err != nil {
		return err
	}

	for _, em := range md {
		m.repository.Manifests.Delete(em.ID)
	}

	return nil
}

// RemovePolicy removes the policy for a given source.
func (m *PolicyManager) RemovePolicy(si SourceInfo) error {
	md := m.repository.Manifests.Find(labelsForSource(si))
	for _, em := range md {
		m.repository.Manifests.Delete(em.ID)
	}

	return nil
}

// GetPolicyByID gets the policy for a given unique ID or ErrPolicyNotFound if not found.
func (m *PolicyManager) GetPolicyByID(id string) (*Policy, error) {
	p := &Policy{}
	if err := m.repository.Manifests.Get(id, &p); err != nil {
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}
	}

	return p, nil
}

// ListPolicies returns a list of all policies.
func (m *PolicyManager) ListPolicies() ([]*Policy, error) {
	ids := m.repository.Manifests.Find(map[string]string{
		"type": "policy",
	})

	var policies []*Policy

	for _, id := range ids {
		pol := &Policy{}
		err := m.repository.Manifests.Get(id.ID, pol)
		if err != nil {
			return nil, err
		}

		md, err := m.repository.Manifests.GetMetadata(id.ID)
		if err != nil {
			return nil, err
		}

		pol.Labels = md.Labels
		pol.Labels["id"] = id.ID
		policies = append(policies, pol)
	}

	return policies, nil
}

func labelsForSource(si SourceInfo) map[string]string {
	switch {
	case si.Path != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "path",
			"username":   si.UserName,
			"hostname":   si.Host,
			"path":       si.Path,
		}
	case si.UserName != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "user",
			"username":   si.UserName,
			"hostname":   si.Host,
		}
	case si.Host != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "host",
			"hostname":   si.Host,
		}
	default:
		return map[string]string{
			"type":       "policy",
			"policyType": "global",
		}
	}

}

// NewPolicyManager creates new policy manager for a given repository.
func NewPolicyManager(r *repo.Repository) *PolicyManager {
	return &PolicyManager{r}
}
