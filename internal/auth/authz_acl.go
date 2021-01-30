package auth

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

const defaultACLRefreshFrequency = 10 * time.Second

// currentUserAtHostMatch is a set of labels that when personalized matches manifests which have
// username equal to current user and hostname matching current host.
var currentUserAtHostMatch = map[string]string{
	"username": acl.CurrentUsernamePlaceholder,
	"hostname": acl.CurrentHostnamePlaceholder,
}

// nonPersonalizedDefaultACL is the default access control list applied when no other ACL is defined.
// The ACL must be personalized to a user account.
var nonPersonalizedDefaultACL = acl.AccessControlList{
	ContentAccess: AccessLevelFull,
	ManifestAccess: map[string][]acl.ManifestAccessRule{
		policy.ManifestType: {
			// everybody can read global policy.
			{
				Match:  map[string]string{"policyType": "global"},
				Access: acl.AccessLevelView,
			},

			// users *@host can see that host's policy.
			{
				Match: map[string]string{
					"policyType": "host",
					"hostname":   acl.CurrentHostnamePlaceholder,
				},
				Access: acl.AccessLevelView,
			},

			// username@hostname has full access to their own policies
			{
				Match:  currentUserAtHostMatch,
				Access: acl.AccessLevelFull,
			},
		},
		snapshot.ManifestType: {
			// username@hostname has full access to their own snapshots
			{
				Match:  currentUserAtHostMatch,
				Access: acl.AccessLevelFull,
			},
		},
		user.ManifestType: {
			// username@hostname has full access to their own user account to be able to change password.
			{
				Match:  currentUserAtHostMatch,
				Access: acl.AccessLevelFull,
			},
		},
	},
}

type aclCache struct {
	aclRefreshFrequency time.Duration

	mu              sync.Mutex
	lastRep         repo.Repository
	nextRefreshTime time.Time
	aclMap          map[acl.Scope]acl.AccessControlList
}

// authorize is an AuthorizerFunc that returns authorizer that uses ACLs stored in the repository.
func (ac *aclCache) authorize(ctx context.Context, rep repo.Repository, usernameAtHostname string) AuthorizationInfo {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	parts := strings.Split(usernameAtHostname, "@")
	if len(parts) != 2 { //nolint:gomnd
		return NoAccess()
	}

	u := parts[0]
	h := parts[1]

	if rep == ac.lastRep {
		// the server switched to another repository, discard cache.
		ac.aclMap = nil
		ac.lastRep = rep
	}

	// see if we're due for a refresh and refresh aclMap
	if clock.Now().After(ac.nextRefreshTime) {
		ac.nextRefreshTime = clock.Now().Add(ac.aclRefreshFrequency)

		newMap, err := acl.LoadACLMap(ctx, rep, ac.aclMap)
		if err != nil {
			log(ctx).Warningf("unable to load aclMap: %v", err)
		} else {
			ac.aclMap = newMap
		}
	}

	// Get ACLs at different scopes, personalize and merge them from most general to most specific.
	// acl.AccessList implements AuthorizationInfo
	return acl.Merge(
		nonPersonalizedDefaultACL.Personalize(u, h),
		ac.aclMap[acl.Scope{}].Personalize(u, h),
		ac.aclMap[acl.Scope{Hostname: h}].Personalize(u, h),
		ac.aclMap[acl.Scope{Username: u}].Personalize(u, h),
		ac.aclMap[acl.Scope{Username: u, Hostname: h}].Personalize(u, h))
}

// ACLAuthorizer returns AuthorizerFunc that will fetch ACLs from the repository
// and evaluate them in the context of current user to determine their permision levels.
func ACLAuthorizer() AuthorizerFunc {
	c := &aclCache{
		aclRefreshFrequency: defaultACLRefreshFrequency,
	}

	return c.authorize
}
