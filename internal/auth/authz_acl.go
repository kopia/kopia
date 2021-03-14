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
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

const defaultACLRefreshFrequency = 10 * time.Second

// ContentRule is a special target rule that targets contents instead of manifests.
var ContentRule = acl.TargetRule{
	"type": "content",
}

// defaultACLPriority is the priority at which default ACLs are specified.
const defaultACLPriority = 50

// anyUser matches any user at any host.
const anyUser = "*@*"

// DefaultACLs specifies default ACLs.
var DefaultACLs = []*acl.Entry{
	{
		// everybody can write contents
		User:     anyUser,
		Target:   ContentRule,
		Access:   acl.AccessLevelAppend,
		Priority: defaultACLPriority,
	},
	{
		// everybody can read global policy.
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:  policy.ManifestType,
			policy.PolicyTypeLabel: policy.PolicyTypeGlobal,
		},
		Access:   AccessLevelRead,
		Priority: defaultACLPriority,
	},
	{
		// users *@host can read own host's policy.
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:  policy.ManifestType,
			policy.PolicyTypeLabel: policy.PolicyTypeHost,
			policy.HostnameLabel:   acl.OwnHost,
		},
		Access:   AccessLevelRead,
		Priority: defaultACLPriority,
	},
	{
		// username@hostname has full access to their own policies
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey: policy.ManifestType,
			policy.UsernameLabel:  acl.OwnUser,
			policy.HostnameLabel:  acl.OwnHost,
		},
		Access:   acl.AccessLevelFull,
		Priority: defaultACLPriority,
	},
	{
		// username@hostname has full access to their own snapshots
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:  snapshot.ManifestType,
			snapshot.UsernameLabel: acl.OwnUser,
			snapshot.HostnameLabel: acl.OwnHost,
		},
		Access:   acl.AccessLevelFull,
		Priority: defaultACLPriority,
	},
	{
		// username@hostname has full access to their user account and can change password
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:        user.ManifestType,
			user.UsernameAtHostnameLabel: acl.OwnUser + "@" + acl.OwnHost,
		},
		Access:   acl.AccessLevelFull,
		Priority: defaultACLPriority,
	},
}

type aclCache struct {
	aclRefreshFrequency time.Duration

	mu              sync.Mutex
	lastRep         repo.Repository
	nextRefreshTime time.Time
	aclEntries      []*acl.Entry
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
		ac.aclEntries = nil
		ac.lastRep = rep

		// ensure ACL entries are reloaded below
		ac.nextRefreshTime = time.Time{}
	}

	// see if we're due for a refresh and refresh aclEntries
	if clock.Now().After(ac.nextRefreshTime) {
		ac.nextRefreshTime = clock.Now().Add(ac.aclRefreshFrequency)

		newMap, err := acl.LoadEntries(ctx, rep, ac.aclEntries)
		if err != nil {
			log(ctx).Warningf("unable to load aclEntries: %v", err)
		} else {
			ac.aclEntries = newMap
		}
	}

	if len(ac.aclEntries) == 0 {
		return legacyAuthorizer{usernameAtHostname}
	}

	return aclEntriesAuthorizer{acl.EntriesForUser(ac.aclEntries, u, h), u, h}
}

type aclEntriesAuthorizer struct {
	entries  []*acl.Entry
	username string
	hostname string
}

func (a aclEntriesAuthorizer) ContentAccessLevel() AccessLevel {
	return acl.EffectivePermissions(a.username, a.hostname, ContentRule, a.entries)
}

func (a aclEntriesAuthorizer) ManifestAccessLevel(labels map[string]string) AccessLevel {
	return acl.EffectivePermissions(a.username, a.hostname, labels, a.entries)
}

// DefaultAuthorizer returns AuthorizerFunc that will fetch ACLs from the repository
// and evaluate them in the context of current user to determine their permision levels.
// It will fall back to legacy authorizer if no ACL entries are defined in the repository.
func DefaultAuthorizer() AuthorizerFunc {
	c := &aclCache{
		aclRefreshFrequency: defaultACLRefreshFrequency,
	}

	return c.authorize
}
