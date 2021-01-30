package auth

import (
	"context"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
)

// AuthorizerFunc gets the authorizations for given user.
type AuthorizerFunc func(ctx context.Context, rep repo.Repository, username string) AuthorizationInfo

// AccessLevel specifies access level when accessing repository objects.
type AccessLevel = acl.AccessLevel

// Access levels forwarded to 'acl' package to allow it to easily implement AuthorizationInfo interface.
const (
	AccessLevelNone   = acl.AccessLevelNone
	AccessLevelRead   = acl.AccessLevelView   // RO access
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
