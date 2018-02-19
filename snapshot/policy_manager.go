package snapshot

import (
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/manifest"
	"github.com/kopia/kopia/repo"
)

const policyPrefix = "P"

// PolicyManager manages snapshotting policies.
type PolicyManager struct {
	repository *repo.Repository
}

// GetEffectivePolicy calculates effective snapshot policy for a given source by combining the source-specifc policy (if any)
// with parent policies. The source must contain a path.
func (m *PolicyManager) GetEffectivePolicy(user, host, path string) (*Policy, error) {
	var md []*manifest.EntryMetadata

	// Find policies applying to paths all the way up to the root.
	for tmpPath := path; len(tmpPath) > 0; {
		md = append(md, m.repository.Manifests.Find(labelsForUserHostPath(user, host, tmpPath))...)

		parentPath := filepath.Dir(tmpPath)
		if parentPath == tmpPath {
			break
		}

		tmpPath = parentPath
	}

	// Try user@host policy
	md = append(md, m.repository.Manifests.Find(labelsForUserHostPath(user, host, ""))...)

	// Try host-level policy.
	md = append(md, m.repository.Manifests.Find(labelsForUserHostPath("", host, ""))...)

	// Global policy.
	md = append(md, m.repository.Manifests.Find(labelsForUserHostPath("", "", ""))...)

	var policies []*Policy
	for _, em := range md {
		p := &Policy{}
		if err := m.repository.Manifests.Get(em.ID, &p); err != nil {
			return nil, fmt.Errorf("got unexpected error when loading policy item %v: %v", em.ID, err)
		}
		policies = append(policies, p)
	}

	return MergePolicies(policies), nil
}

// GetDefinedPolicy returns the policy defined on the provided (user, host, path) or ErrPolicyNotFound if not present.
func (m *PolicyManager) GetDefinedPolicy(user, host, path string) (*Policy, error) {
	md := m.repository.Manifests.Find(labelsForUserHostPath(user, host, path))

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

// SetPolicy sets the policy on (user, host, path).
func (m *PolicyManager) SetPolicy(user, host, path string, pol *Policy) error {
	md := m.repository.Manifests.Find(labelsForUserHostPath(user, host, path))

	if _, err := m.repository.Manifests.Put(labelsForUserHostPath(user, host, path), pol); err != nil {
		return err
	}

	for _, em := range md {
		m.repository.Manifests.Delete(em.ID)
	}

	return nil
}

// RemovePolicy removes the policy for (user, host, path).
func (m *PolicyManager) RemovePolicy(user, host, path string) error {
	md := m.repository.Manifests.Find(labelsForUserHostPath(user, host, path))
	for _, em := range md {
		m.repository.Manifests.Delete(em.ID)
	}

	return nil
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

func labelsForUserHostPath(user, host, path string) map[string]string {
	switch {
	case path != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "path",
			"username":   user,
			"hostname":   host,
			"path":       path,
		}
	case user != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "user",
			"username":   user,
			"hostname":   host,
		}
	case host != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "host",
			"hostname":   host,
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
