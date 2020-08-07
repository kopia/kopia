// Package policy implements management of snapshot policies.
package policy

import (
	"context"
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
func GetEffectivePolicy(ctx context.Context, rep repo.Repository, si snapshot.SourceInfo) (effective *Policy, sources []*Policy, e error) {
	var md []*manifest.EntryMetadata

	// Find policies applying to paths all the way up to the root.
	for tmp := si; len(si.Path) > 0; {
		manifests, err := rep.FindManifests(ctx, labelsForSource(tmp))
		if err != nil {
			return nil, nil, err
		}

		md = append(md, manifests...)

		parentPath := getParentPathOSIndependent(tmp.Path)
		if parentPath == tmp.Path {
			break
		}

		tmp.Path = parentPath
	}

	// Try user@host policy
	userHostManifests, err := rep.FindManifests(ctx, labelsForSource(snapshot.SourceInfo{Host: si.Host, UserName: si.UserName}))
	if err != nil {
		return nil, nil, err
	}

	md = append(md, userHostManifests...)

	// Try host-level policy.
	hostManifests, err := rep.FindManifests(ctx, labelsForSource(snapshot.SourceInfo{Host: si.Host}))
	if err != nil {
		return nil, nil, err
	}

	md = append(md, hostManifests...)

	// Global policy.
	globalManifests, err := rep.FindManifests(ctx, labelsForSource(GlobalPolicySourceInfo))
	if err != nil {
		return nil, nil, err
	}

	md = append(md, globalManifests...)

	var policies []*Policy

	for _, em := range md {
		p := &Policy{}
		if err := loadPolicyFromManifest(ctx, rep, em.ID, p); err != nil {
			return nil, nil, errors.Wrapf(err, "got unexpected error when loading policy item %v", em.ID)
		}

		policies = append(policies, p)
		log(ctx).Debugf("loaded parent policy for %v: %v", si, p.Target())
	}

	merged := MergePolicies(policies)
	merged.Labels = labelsForSource(si)

	return merged, policies, nil
}

// GetDefinedPolicy returns the policy defined on the provided snapshot.SourceInfo or ErrPolicyNotFound if not present.
func GetDefinedPolicy(ctx context.Context, rep repo.Repository, si snapshot.SourceInfo) (*Policy, error) {
	md, err := rep.FindManifests(ctx, labelsForSource(si))
	if err != nil {
		return nil, errors.Wrap(err, "unable to find policy for source")
	}

	if len(md) == 0 {
		return nil, ErrPolicyNotFound
	}

	// arbitrality pick first pick ID to return in case there's more than one
	// this is possible when two repository clients independently create manifests at approximately the same time
	// so it should not really matter which one we pick.
	// see https://github.com/kopia/kopia/issues/391
	manifestID := md[0].ID

	p := &Policy{}

	if err := loadPolicyFromManifest(ctx, rep, manifestID, p); err != nil {
		return nil, err
	}

	return p, nil
}

// SetPolicy sets the policy on a given source.
func SetPolicy(ctx context.Context, rep repo.Repository, si snapshot.SourceInfo, pol *Policy) error {
	md, err := rep.FindManifests(ctx, labelsForSource(si))
	if err != nil {
		return errors.Wrapf(err, "unable to load manifests for %v", si)
	}

	if _, err := rep.PutManifest(ctx, labelsForSource(si), pol); err != nil {
		return err
	}

	for _, em := range md {
		if err := rep.DeleteManifest(ctx, em.ID); err != nil {
			return errors.Wrap(err, "unable to delete previous policy manifest")
		}
	}

	return nil
}

// RemovePolicy removes the policy for a given source.
func RemovePolicy(ctx context.Context, rep repo.Repository, si snapshot.SourceInfo) error {
	md, err := rep.FindManifests(ctx, labelsForSource(si))
	if err != nil {
		return errors.Wrapf(err, "unable to load manifests for %v", si)
	}

	for _, em := range md {
		if err := rep.DeleteManifest(ctx, em.ID); err != nil {
			return errors.Wrap(err, "unable to delete previous manifest")
		}
	}

	return nil
}

// GetPolicyByID gets the policy for a given unique ID or ErrPolicyNotFound if not found.
func GetPolicyByID(ctx context.Context, rep repo.Repository, id manifest.ID) (*Policy, error) {
	p := &Policy{}
	if err := loadPolicyFromManifest(ctx, rep, id, p); err != nil {
		return nil, err
	}

	return p, nil
}

// ListPolicies returns a list of all policies.
func ListPolicies(ctx context.Context, rep repo.Repository) ([]*Policy, error) {
	ids, err := rep.FindManifests(ctx, map[string]string{
		typeKey: "policy",
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to list manifests")
	}

	var policies []*Policy

	for _, id := range ids {
		pol := &Policy{}

		if err := loadPolicyFromManifest(ctx, rep, id.ID, pol); err != nil {
			return nil, err
		}

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
func TreeForSource(ctx context.Context, rep repo.Repository, si snapshot.SourceInfo) (*Tree, error) {
	pols, err := applicablePoliciesForSource(ctx, rep, si)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get policies")
	}

	return BuildTree(pols, DefaultPolicy), nil
}

func applicablePoliciesForSource(ctx context.Context, rep repo.Repository, si snapshot.SourceInfo) (map[string]*Policy, error) {
	result := map[string]*Policy{}

	pol, _, err := GetEffectivePolicy(ctx, rep, si)
	if err != nil {
		return nil, err
	}

	result["."] = pol

	// Find all policies for this host and user
	policies, err := rep.FindManifests(ctx, map[string]string{
		typeKey:      "policy",
		"policyType": "path",
		"username":   si.UserName,
		"hostname":   si.Host,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "unable to find manifests for %v@%v", si.UserName, si.Host)
	}

	log(ctx).Debugf("found %v policies for %v@%v", len(policies), si.UserName, si.Host)

	for _, id := range policies {
		pol := &Policy{}

		err := loadPolicyFromManifest(ctx, rep, id.ID, pol)
		if err != nil {
			return nil, err
		}

		policyPath := pol.Labels["path"]

		rel := nestedRelativePathNormalizedToSlashes(si.Path, policyPath)
		if rel == "" {
			log(ctx).Debugf("%v is not under %v", policyPath, si.Path)
			continue
		}

		rel = "./" + rel
		log(ctx).Debugf("loading policy for %v (%v)", policyPath, rel)

		result[rel] = pol
	}

	return result, nil
}

func loadPolicyFromManifest(ctx context.Context, rep repo.Repository, id manifest.ID, pol *Policy) error {
	md, err := rep.GetManifest(ctx, id, pol)
	if err != nil {
		if errors.Is(err, manifest.ErrNotFound) {
			return ErrPolicyNotFound
		}

		return err
	}

	pol.Labels = md.Labels
	pol.Labels["id"] = string(md.ID)

	return nil
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

func getParentPathOSIndependent(p string) string {
	// split into volume (Windows only, e.g. X:) and path using either slash or backslash.
	vol, pth := volumeAndPath(p)

	last := strings.LastIndexAny(pth, "/\\")
	if last == len(pth)-1 && last != 0 {
		pth = pth[0:last]
		last = strings.LastIndexAny(pth, "/\\")
	}

	if last < 0 {
		return p
	}

	// special case for root, return root path itself (either slash or backslash-separated)
	if last == 0 {
		return vol + pth[0:1]
	}

	return vol + pth[0:last]
}

// volumeAndPath splits path 'p' into Windows-specific volume (e.g. "X:" and path after that starting with either slash or backslash)
func volumeAndPath(p string) (vol, path string) {
	if len(p) >= 3 && p[1] == ':' && isSlashOrBackslash(p[2]) {
		// "X:\"
		return p[0:2], p[2:]
	}

	return "", p
}

func isSlashOrBackslash(c uint8) bool {
	return c == '/' || c == '\\'
}

func isWindowsStylePath(p string) bool {
	v, _ := volumeAndPath(p)
	return v != ""
}

func trimTrailingSlashOrBackslash(path string) string {
	return strings.TrimSuffix(strings.TrimSuffix(path, "/"), "\\")
}

func nestedRelativePathNormalizedToSlashes(parent, child string) string {
	isWin := isWindowsStylePath(parent)
	parent = trimTrailingSlashOrBackslash(parent)

	if !strings.HasPrefix(child, parent+"/") && !strings.HasPrefix(child, parent+"\\") {
		return ""
	}

	p := strings.TrimPrefix(child, parent)[1:]

	if isWin {
		return strings.ReplaceAll(p, "\\", "/")
	}

	return p
}
