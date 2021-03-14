package auth

import (
	"context"
	"strings"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// AuthorizerFunc gets the authorizations for given user.
type AuthorizerFunc func(ctx context.Context, rep repo.Repository, username string) AuthorizationInfo

// AccessLevel specifies access level when accessing repository objects.
type AccessLevel = acl.AccessLevel

// Access levels forwarded to 'acl' package to allow it to easily implement AuthorizationInfo interface.
const (
	AccessLevelNone   = acl.AccessLevelNone
	AccessLevelRead   = acl.AccessLevelRead   // RO access
	AccessLevelAppend = acl.AccessLevelAppend // RO + create new
	AccessLevelFull   = acl.AccessLevelFull   // read/write/delete
)

// AuthorizationInfo determines logged in user's access level.
type AuthorizationInfo interface {
	// ContentAccessLevel determines whether the user can read/write contents.
	ContentAccessLevel() AccessLevel

	// ManifestAccessLevel determines whether the user has access to a manifest with given labels.
	ManifestAccessLevel(labels map[string]string) AccessLevel
}

type noAccessAuthorizer struct{}

func (noAccessAuthorizer) ContentAccessLevel() AccessLevel { return AccessLevelNone }
func (noAccessAuthorizer) ManifestAccessLevel(labels map[string]string) AccessLevel {
	return AccessLevelNone
}

// NoAccess returns AuthorizationInfo which grants no permissions.
func NoAccess() AuthorizationInfo {
	return noAccessAuthorizer{}
}

type legacyAuthorizer struct {
	usernameAtHostname string
}

func (la legacyAuthorizer) ContentAccessLevel() AccessLevel { return AccessLevelFull }
func (la legacyAuthorizer) ManifestAccessLevel(labels map[string]string) AccessLevel {
	if labels[manifest.TypeLabelKey] == policy.ManifestType {
		// everybody can read global policy.
		switch labels[policy.PolicyTypeLabel] {
		case policy.PolicyTypeGlobal:
			return AccessLevelRead

		case policy.PolicyTypeHost:
			if strings.HasSuffix(la.usernameAtHostname, "@"+labels[snapshot.HostnameLabel]) {
				return AccessLevelRead
			}
		}
	}

	// full access to policies/snapshots for the username@hostname
	if labels[snapshot.UsernameLabel]+"@"+labels[snapshot.HostnameLabel] == la.usernameAtHostname {
		return AccessLevelFull
	}

	// no access otherwise
	return AccessLevelNone
}

// LegacyAuthorizerForUser is an AuthorizerFunc that returns authorizer with legacy (pre-ACL)
// authorization rules (authenticated users can see their own snapshots/policies only).
func LegacyAuthorizerForUser(ctx context.Context, rep repo.Repository, usernameAtHostname string) AuthorizationInfo {
	return legacyAuthorizer{usernameAtHostname}
}

var _ AuthorizerFunc = LegacyAuthorizerForUser
