// Package policy implements management of snapshot policies.
package policy

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
)

const typeKey = manifest.TypeLabelKey

// GlobalPolicySourceInfo is a source where global policy is attached.
var GlobalPolicySourceInfo = snapshot.SourceInfo{}

var log = logging.GetContextLoggerFunc("kopia/snapshot/policy")

// GetEffectivePolicy calculates effective snapshot policy for a given source by combining the source-specifc policy (if any)
// with parent policies. The source must contain a path.
// Returns the effective policies and all source policies that contributed to that (most specific first).
func GetEffectivePolicy(ctx context.Context, rep *repo.Repository, si snapshot.SourceInfo) (effective *Policy, sources []*Policy, e error) {
	var md []*manifest.EntryMetadata

	// Find policies applying to paths all the way up to the root.
	for tmp := si; len(si.Path) > 0; {
		manifests, err := rep.Manifests.Find(ctx, labelsForSource(tmp))
		if err != nil {
			return nil, nil, err
		}

		md = append(md, manifests...)

		parentPath := filepath.Dir(tmp.Path)
		if parentPath == tmp.Path {
			break
		}

		tmp.Path = parentPath
	}

	// Try user@host policy
	userHostManifests, err := rep.Manifests.Find(ctx, labelsForSource(snapshot.SourceInfo{Host: si.Host, UserName: si.UserName}))
	if err != nil {
		return nil, nil, err
	}

	md = append(md, userHostManifests...)

	// Try host-level policy.
	hostManifests, err := rep.Manifests.Find(ctx, labelsForSource(snapshot.SourceInfo{Host: si.Host}))
	if err != nil {
		return nil, nil, err
	}

	md = append(md, hostManifests...)

	// Global policy.
	globalManifests, err := rep.Manifests.Find(ctx, labelsForSource(GlobalPolicySourceInfo))
	if err != nil {
		return nil, nil, err
	}

	md = append(md, globalManifests...)

	var policies []*Policy

	for _, em := range md {
		p := &Policy{}
		if err := rep.Manifests.Get(ctx, em.ID, &p); err != nil {
			return nil, nil, errors.Wrapf(err, "got unexpected error when loading policy item %v", em.ID)
		}

		p.Labels = em.Labels
		policies = append(policies, p)
		log(ctx).Debugf("loaded parent policy for %v: %v", si, p.Target())
	}

	merged := MergePolicies(policies)
	merged.Labels = labelsForSource(si)

	return merged, policies, nil
}

// GetDefinedPolicy returns the policy defined on the provided snapshot.SourceInfo or ErrPolicyNotFound if not present.
func GetDefinedPolicy(ctx context.Context, rep *repo.Repository, si snapshot.SourceInfo) (*Policy, error) {
	md, err := rep.Manifests.Find(ctx, labelsForSource(si))
	if err != nil {
		return nil, errors.Wrap(err, "unable to find policy for source")
	}

	if len(md) == 0 {
		return nil, ErrPolicyNotFound
	}

	if len(md) == 1 {
		p := &Policy{}

		err := rep.Manifests.Get(ctx, md[0].ID, p)
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}

		if err != nil {
			return nil, err
		}

		em, err := rep.Manifests.GetMetadata(ctx, md[0].ID)
		if err != nil {
			return nil, ErrPolicyNotFound
		}

		p.Labels = em.Labels

		return p, nil
	}

	return nil, errors.New("ambiguous policy")
}

// SetPolicy sets the policy on a given source.
func SetPolicy(ctx context.Context, rep *repo.Repository, si snapshot.SourceInfo, pol *Policy) error {
	md, err := rep.Manifests.Find(ctx, labelsForSource(si))
	if err != nil {
		return errors.Wrapf(err, "unable to load manifests for %v", si)
	}

	if _, err := rep.Manifests.Put(ctx, labelsForSource(si), pol); err != nil {
		return err
	}

	for _, em := range md {
		if err := rep.Manifests.Delete(ctx, em.ID); err != nil {
			return errors.Wrap(err, "unable to delete previous policy manifest")
		}
	}

	return nil
}

// RemovePolicy removes the policy for a given source.
func RemovePolicy(ctx context.Context, rep *repo.Repository, si snapshot.SourceInfo) error {
	md, err := rep.Manifests.Find(ctx, labelsForSource(si))
	if err != nil {
		return errors.Wrapf(err, "unable to load manifests for %v", si)
	}

	for _, em := range md {
		if err := rep.Manifests.Delete(ctx, em.ID); err != nil {
			return errors.Wrap(err, "unable to delete previous manifest")
		}
	}

	return nil
}

// GetPolicyByID gets the policy for a given unique ID or ErrPolicyNotFound if not found.
func GetPolicyByID(ctx context.Context, rep *repo.Repository, id manifest.ID) (*Policy, error) {
	p := &Policy{}
	if err := rep.Manifests.Get(ctx, id, &p); err != nil {
		if err == manifest.ErrNotFound {
			return nil, ErrPolicyNotFound
		}
	}

	return p, nil
}

// ListPolicies returns a list of all policies.
func ListPolicies(ctx context.Context, rep *repo.Repository) ([]*Policy, error) {
	ids, err := rep.Manifests.Find(ctx, map[string]string{
		typeKey: "policy",
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list manifests")
	}

	var policies []*Policy

	for _, id := range ids {
		pol := &Policy{}

		if err := rep.Manifests.Get(ctx, id.ID, pol); err != nil {
			return nil, err
		}

		md, err := rep.Manifests.GetMetadata(ctx, id.ID)
		if err != nil {
			return nil, err
		}

		pol.Labels = md.Labels
		pol.Labels["id"] = string(id.ID)
		policies = append(policies, pol)
	}

	return policies, nil
}

// SubdirectoryPolicyMap implements Getter for a static mapping of relative paths to Policy for subdirectories
type SubdirectoryPolicyMap map[string]*Policy

// GetPolicyForPath returns Policy defined in the map or nil.
func (m SubdirectoryPolicyMap) GetPolicyForPath(relativePath string) (*Policy, error) {
	return m[relativePath], nil
}

// TreeForSource returns policy Tree for a given source.
func TreeForSource(ctx context.Context, rep *repo.Repository, si snapshot.SourceInfo) (*Tree, error) {
	result := map[string]*Policy{}

	pol, _, err := GetEffectivePolicy(ctx, rep, si)
	if err != nil {
		return nil, err
	}

	result["."] = pol

	// Find all policies for this host and user
	policies, err := rep.Manifests.Find(ctx, map[string]string{
		typeKey:      "policy",
		"policyType": "path",
		"username":   si.UserName,
		"hostname":   si.Host,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to find manifests for %v@%v", si.UserName, si.Host)
	}

	log(ctx).Debugf("found %v policies for %v@%v", si.UserName, si.Host)

	for _, id := range policies {
		em, err := rep.Manifests.GetMetadata(ctx, id.ID)
		if err != nil {
			return nil, err
		}

		policyPath := em.Labels["path"]

		if !strings.HasPrefix(policyPath, si.Path+"/") {
			continue
		}

		rel, err := filepath.Rel(si.Path, policyPath)
		if err != nil {
			return nil, errors.Wrap(err, "unable to determine relative path")
		}

		rel = "./" + rel
		log(ctx).Debugf("loading policy for %v (%v)", policyPath, rel)

		pol := &Policy{}
		if err := rep.Manifests.Get(ctx, id.ID, pol); err != nil {
			return nil, errors.Wrapf(err, "unable to load policy %v", id.ID)
		}

		result[rel] = pol
	}

	return BuildTree(result, DefaultPolicy), nil
}

func labelsForSource(si snapshot.SourceInfo) map[string]string {
	switch {
	case si.Path != "":
		return map[string]string{
			typeKey:      "policy",
			"policyType": "path",
			"username":   si.UserName,
			"hostname":   si.Host,
			"path":       si.Path,
		}
	case si.UserName != "":
		return map[string]string{
			typeKey:      "policy",
			"policyType": "user",
			"username":   si.UserName,
			"hostname":   si.Host,
		}
	case si.Host != "":
		return map[string]string{
			typeKey:      "policy",
			"policyType": "host",
			"hostname":   si.Host,
		}
	default:
		return map[string]string{
			typeKey:      "policy",
			"policyType": "global",
		}
	}
}
