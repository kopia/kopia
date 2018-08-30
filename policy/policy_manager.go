// Package policy implements management of snapshot policies.
package policy

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/kopialogging"
	"github.com/kopia/kopia/manifest"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

// GlobalPolicySourceInfo is a source where global policy is attached.
var GlobalPolicySourceInfo = snapshot.SourceInfo{}

var log = kopialogging.Logger("kopia/policy")

// GetEffectivePolicy calculates effective snapshot policy for a given source by combining the source-specifc policy (if any)
// with parent policies. The source must contain a path.
// Returns the effective policies and all source policies that contributed to that (most specific first).
func GetEffectivePolicy(rep *repo.Repository, si snapshot.SourceInfo) (*Policy, []*Policy, error) {
	var md []*manifest.EntryMetadata

	// Find policies applying to paths all the way up to the root.
	for tmp := si; len(si.Path) > 0; {
		manifests := rep.Manifests.Find(labelsForSource(tmp))
		md = append(md, manifests...)

		parentPath := filepath.Dir(tmp.Path)
		if parentPath == tmp.Path {
			break
		}

		tmp.Path = parentPath
	}

	// Try user@host policy
	md = append(md, rep.Manifests.Find(labelsForSource(snapshot.SourceInfo{Host: si.Host, UserName: si.UserName}))...)

	// Try host-level policy.
	md = append(md, rep.Manifests.Find(labelsForSource(snapshot.SourceInfo{Host: si.Host}))...)

	// Global policy.
	globalManifests := rep.Manifests.Find(labelsForSource(GlobalPolicySourceInfo))
	md = append(md, globalManifests...)

	var policies []*Policy
	for _, em := range md {
		p := &Policy{}
		if err := rep.Manifests.Get(em.ID, &p); err != nil {
			return nil, nil, fmt.Errorf("got unexpected error when loading policy item %v: %v", em.ID, err)
		}
		p.Labels = em.Labels
		policies = append(policies, p)
		log.Debugf("loaded parent policy for %v: %v", si, p.Target())
	}

	merged := MergePolicies(policies)
	merged.Labels = labelsForSource(si)

	return merged, policies, nil
}

// GetDefinedPolicy returns the policy defined on the provided snapshot.SourceInfo or ErrPolicyNotFound if not present.
func GetDefinedPolicy(rep *repo.Repository, si snapshot.SourceInfo) (*Policy, error) {
	md := rep.Manifests.Find(labelsForSource(si))

	if len(md) == 0 {
		return nil, ErrPolicyNotFound
	}

	if len(md) == 1 {
		p := &Policy{}

		err := rep.Manifests.Get(md[0].ID, p)
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}

		if err != nil {
			return nil, err
		}

		em, err := rep.Manifests.GetMetadata(md[0].ID)
		if err != nil {
			return nil, ErrPolicyNotFound
		}

		p.Labels = em.Labels
		return p, nil
	}

	return nil, fmt.Errorf("ambiguous policy")
}

// SetPolicy sets the policy on a given source.
func SetPolicy(rep *repo.Repository, si snapshot.SourceInfo, pol *Policy) error {
	md := rep.Manifests.Find(labelsForSource(si))

	if _, err := rep.Manifests.Put(labelsForSource(si), pol); err != nil {
		return err
	}

	for _, em := range md {
		rep.Manifests.Delete(em.ID)
	}

	return nil
}

// RemovePolicy removes the policy for a given source.
func RemovePolicy(rep *repo.Repository, si snapshot.SourceInfo) error {
	md := rep.Manifests.Find(labelsForSource(si))
	for _, em := range md {
		rep.Manifests.Delete(em.ID)
	}

	return nil
}

// GetPolicyByID gets the policy for a given unique ID or ErrPolicyNotFound if not found.
func GetPolicyByID(rep *repo.Repository, id string) (*Policy, error) {
	p := &Policy{}
	if err := rep.Manifests.Get(id, &p); err != nil {
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}
	}

	return p, nil
}

// ListPolicies returns a list of all policies.
func ListPolicies(rep *repo.Repository) ([]*Policy, error) {
	ids := rep.Manifests.Find(map[string]string{
		"type": "policy",
	})

	var policies []*Policy

	for _, id := range ids {
		pol := &Policy{}
		err := rep.Manifests.Get(id.ID, pol)
		if err != nil {
			return nil, err
		}

		md, err := rep.Manifests.GetMetadata(id.ID)
		if err != nil {
			return nil, err
		}

		pol.Labels = md.Labels
		pol.Labels["id"] = id.ID
		policies = append(policies, pol)
	}

	return policies, nil
}

// FilesPolicyGetter returns ignorefs.FilesPolicyGetter for a given source.
func FilesPolicyGetter(rep *repo.Repository, si snapshot.SourceInfo) (ignorefs.FilesPolicyGetter, error) {
	result := ignorefs.FilesPolicyMap{}

	pol, _, err := GetEffectivePolicy(rep, si)
	if err != nil {
		return nil, err
	}

	result["."] = &pol.FilesPolicy

	// Find all policies for this host and user
	policies := rep.Manifests.Find(map[string]string{
		"type":       "policy",
		"policyType": "path",
		"username":   si.UserName,
		"hostname":   si.Host,
	})

	log.Debugf("found %v policies for %v@%v", si.UserName, si.Host)

	for _, id := range policies {
		em, err := rep.Manifests.GetMetadata(id.ID)
		if err != nil {
			return nil, err
		}

		policyPath := em.Labels["path"]

		if !strings.HasPrefix(policyPath, si.Path+"/") {
			continue
		}

		rel, err := filepath.Rel(si.Path, policyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to determine relative path: %v", err)
		}
		rel = "./" + rel
		log.Debugf("loading policy for %v (%v)", policyPath, rel)
		pol := &Policy{}
		if err := rep.Manifests.Get(id.ID, pol); err != nil {
			return nil, fmt.Errorf("unable to load policy %v: %v", id.ID, err)
		}
		result[rel] = &pol.FilesPolicy
	}

	return result, nil
}

func labelsForSource(si snapshot.SourceInfo) map[string]string {
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
