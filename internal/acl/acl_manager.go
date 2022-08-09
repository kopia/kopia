// Package acl provides management of ACLs that define permissions granted to repository users.
package acl

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

const (
	aclManifestType = "acl"
)

func matchOrWildcard(rule, actual string) bool {
	if rule == "*" {
		return true
	}

	return rule == actual
}

func userMatches(rule, username, hostname string) bool {
	ruleParts := strings.Split(rule, "@")
	if len(ruleParts) != 2 { //nolint:gomnd
		return false
	}

	return matchOrWildcard(ruleParts[0], username) && matchOrWildcard(ruleParts[1], hostname)
}

// EntriesForUser computes the list of ACL entries matching the given user.
func EntriesForUser(entries []*Entry, username, hostname string) []*Entry {
	result := []*Entry{}

	for _, e := range entries {
		if userMatches(e.User, username, hostname) {
			result = append(result, e)
		}
	}

	return result
}

// EffectivePermissions computes the effective access level for a given user@hostname to subject
// for a given set of ACL Entries.
func EffectivePermissions(username, hostname string, target map[string]string, entries []*Entry) AccessLevel {
	highest := AccessLevelNone

	for _, e := range entries {
		if !userMatches(e.User, username, hostname) {
			continue
		}

		if !e.Target.matches(target, username, hostname) {
			continue
		}

		if e.Access > highest {
			highest = e.Access
		}
	}

	return highest
}

// LoadEntries returns the set of all ACLs in the repository, using old list as a cache.
func LoadEntries(ctx context.Context, rep repo.Repository, old []*Entry) ([]*Entry, error) {
	if rep == nil {
		return nil, nil
	}

	entries, err := rep.FindManifests(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error listing ACL manifests")
	}

	om := map[manifest.ID]*Entry{}
	for _, v := range old {
		om[v.ManifestID] = v
	}

	result := []*Entry{}

	for _, m := range entries {
		if o := om[m.ID]; o != nil {
			result = append(result, o)
			continue
		}

		var p Entry

		_, err := rep.GetManifest(ctx, m.ID, &p)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading ACL manifest %v", m.ID)
		}

		p.ManifestID = m.ID

		result = append(result, &p)
	}

	return result, nil
}

// AddACL validates and adds the specified ACL entry to the repository.
func AddACL(ctx context.Context, w repo.RepositoryWriter, e *Entry) error {
	if err := e.Validate(); err != nil {
		return errors.Wrap(err, "error validating ACL")
	}

	manifestID, err := w.PutManifest(ctx, map[string]string{
		manifest.TypeLabelKey: aclManifestType,
	}, e)
	if err != nil {
		return errors.Wrap(err, "error writing manifest")
	}

	e.ManifestID = manifestID

	return nil
}
