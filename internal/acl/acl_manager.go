// Package acl provides management of ACLs that define permissions granted to repository users.
package acl

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

const (
	aclManifestType = "acl"
	usernameLabel   = "username"
	hostnameLabel   = "hostname"
)

// Scope defines scope to which the ACL is applicable.
type Scope struct {
	Username string // empty == all users
	Hostname string // empty == all hosts
}

// LoadACLMap returns the map of all ACLs in the repository by their Scope, using old map as a cache.
func LoadACLMap(ctx context.Context, rep repo.Repository, old map[Scope]AccessControlList) (map[Scope]AccessControlList, error) {
	if rep == nil {
		return nil, nil
	}

	entries, err := rep.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error listing ACL manifests")
	}

	result := map[Scope]AccessControlList{}

	for _, m := range manifest.DedupeEntryMetadataByLabels(entries, usernameLabel, hostnameLabel) {
		scope := Scope{m.Labels[usernameLabel], m.Labels[hostnameLabel]}

		// same scope info as before
		if o, ok := old[scope]; ok && o.ManifestID == m.ID {
			result[scope] = o
			continue
		}

		var p AccessControlList

		_, err := rep.GetManifest(ctx, m.ID, &p)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading ACL manifest %v", scope)
		}

		p.ManifestID = m.ID

		result[scope] = p
	}

	return result, nil
}

// GetACL returns the ACL for a given scope.
func GetACL(ctx context.Context, r repo.Repository, scope Scope) (AccessControlList, error) {
	manifests, err := r.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
		usernameLabel:         scope.Username,
		hostnameLabel:         scope.Hostname,
	})
	if err != nil {
		return AccessControlList{}, errors.Wrap(err, "error looking for ACL")
	}

	if len(manifests) == 0 {
		return AccessControlList{}, nil
	}

	p := AccessControlList{}
	if _, err := r.GetManifest(ctx, manifest.PickLatestID(manifests), &p); err != nil {
		return AccessControlList{}, errors.Wrap(err, "error loading ACL")
	}

	return p, nil
}

// SetACL creates or updates ACL for a given scope.
func SetACL(ctx context.Context, w repo.RepositoryWriter, scope Scope, p AccessControlList) error {
	manifests, err := w.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
		usernameLabel:         scope.Username,
		hostnameLabel:         scope.Hostname,
	})
	if err != nil {
		return errors.Wrap(err, "error looking for ACL")
	}

	id, err := w.PutManifest(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
		usernameLabel:         scope.Username,
		hostnameLabel:         scope.Hostname,
	}, p)
	if err != nil {
		return errors.Wrap(err, "error writing ACL")
	}

	for _, m := range manifests {
		if err := w.DeleteManifest(ctx, m.ID); err != nil {
			return errors.Wrapf(err, "error deleting ACL %v", scope)
		}
	}

	p.ManifestID = id

	return nil
}

// DeleteACL removes ACL with a given scope.
func DeleteACL(ctx context.Context, w repo.RepositoryWriter, scope Scope) error {
	manifests, err := w.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
		usernameLabel:         scope.Username,
		hostnameLabel:         scope.Hostname,
	})
	if err != nil {
		return errors.Wrap(err, "error looking for ACL")
	}

	for _, m := range manifests {
		if err := w.DeleteManifest(ctx, m.ID); err != nil {
			return errors.Wrapf(err, "error deleting ACL %v", scope)
		}
	}

	return nil
}
