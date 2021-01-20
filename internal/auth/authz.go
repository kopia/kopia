package auth

import (
	"context"
	"strings"

	"github.com/kopia/kopia/repo"
)

// AuthorizerFunc gets the authorizations for given user.
type AuthorizerFunc func(ctx context.Context, rep repo.Repository, username string) AuthorizationInfo

// AccessLevel specifies access level when accessing repository objects.
type AccessLevel int

// Access levels.
const (
	AccessLevelNone   AccessLevel = iota
	AccessLevelRead               // RO access
	AccessLevelAppend             // RO + create new
	AccessLevelFull               // read/write/delete
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

// NoAccess implements AuthorizationInfo which grants no permissions.
var NoAccess AuthorizationInfo = noAccessAuthorizer{}

type legacyAuthorizer struct {
	usernameAtHostname string
}

func (la legacyAuthorizer) ContentAccessLevel() AccessLevel { return AccessLevelFull }
func (la legacyAuthorizer) ManifestAccessLevel(labels map[string]string) AccessLevel {
	if labels["type"] == "policy" {
		// everybody can read global policy.
		switch labels["policyType"] {
		case "global":
			return AccessLevelRead

		case "host":
			if strings.HasSuffix(la.usernameAtHostname, "@"+labels["hostname"]) {
				return AccessLevelRead
			}
		}
	}

	// full access to policies/snapshots for the username@hostname
	if labels["username"]+"@"+labels["hostname"] == la.usernameAtHostname {
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
