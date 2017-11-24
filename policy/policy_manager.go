package policy

import (
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/manifest"

	"github.com/kopia/kopia/repo"
)

// SourceInfo represents the information about snapshot source.
type SourceInfo struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

const policyPrefix = "P"

// Manager manages snapshotting policies.
type Manager struct {
	repository *repo.Repository
}

// GetEffectivePolicy calculates effective snapshot policy for a given source by combining the source-specifc policy (if any)
// with parent policies. The source must contain a path.
func (m *Manager) GetEffectivePolicy(user, host, path string) (*Policy, error) {
	var ids []string

	// Find policies applying to paths all the way up to the root.
	for tmpPath := path; len(tmpPath) > 0; {
		ids = append(ids, m.repository.Manifests.Find(labelsForUserHostPath(user, host, tmpPath))...)

		parentPath := filepath.Dir(tmpPath)
		if parentPath == tmpPath {
			break
		}

		tmpPath = parentPath
	}

	// Try user@host policy
	ids = append(ids, m.repository.Manifests.Find(labelsForUserHostPath(user, host, ""))...)

	// Try host-level policy.
	ids = append(ids, m.repository.Manifests.Find(labelsForUserHostPath("", host, ""))...)

	// Global policy.
	ids = append(ids, m.repository.Manifests.Find(labelsForUserHostPath("", "", ""))...)

	var policies []*Policy
	for _, id := range ids {
		p := &Policy{}
		if _, err := m.repository.Manifests.Get(id, &p); err != nil {
			return nil, fmt.Errorf("got unexpected error when loading policy item %v: %v", id, err)
		}
		policies = append(policies, p)
	}

	return MergePolicies(policies), nil
}

func (m *Manager) GetDefinedPolicy(user, host, path string) (*Policy, error) {
	ids := m.repository.Manifests.Find(labelsForUserHostPath(user, host, path))

	if len(ids) == 0 {
		return nil, ErrPolicyNotFound
	}

	if len(ids) == 1 {
		p := &Policy{}

		labels, err := m.repository.Manifests.Get(ids[0], p)
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}

		if err != nil {
			return nil, err
		}

		p.Labels = labels
		return p, nil
	}

	return nil, fmt.Errorf("ambiguous policy")
}

func (m *Manager) SetPolicy(user, host, path string, pol *Policy) error {
	ids := m.repository.Manifests.Find(labelsForUserHostPath(user, host, path))

	if _, err := m.repository.Manifests.Add(labelsForUserHostPath(user, host, path), pol); err != nil {
		return err
	}

	for _, id := range ids {
		m.repository.Manifests.Delete(id)
	}

	return nil
}

func (m *Manager) RemovePolicy(user, host, path string) error {
	ids := m.repository.Manifests.Find(labelsForUserHostPath(user, host, path))
	for _, id := range ids {
		m.repository.Manifests.Delete(id)
	}

	return nil
}

// ListPolicies returns a list of all snapshot policies.
func (m *Manager) ListPolicies() ([]*Policy, error) {
	ids := m.repository.Manifests.Find(map[string]string{
		"type": "policy",
	})

	var policies []*Policy

	for _, id := range ids {
		pol := &Policy{}
		labels, err := m.repository.Manifests.Get(id, pol)
		if err != nil {
			return nil, err
		}

		pol.Labels = labels
		pol.Labels["id"] = id
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
			"user":       user,
			"host":       host,
			"path":       path,
		}
	case user != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "user",
			"user":       user,
			"host":       host,
		}
	case host != "":
		return map[string]string{
			"type":       "policy",
			"policyType": "host",
			"host":       host,
		}
	default:
		return map[string]string{
			"type":       "policy",
			"policyType": "global",
		}
	}

}

// NewManager creates new policy manager for a given repository.
func NewManager(r *repo.Repository) *Manager {
	return &Manager{r}
}
