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
//
//nolint:gochecknoglobals
var ContentRule = acl.TargetRule{
	manifest.TypeLabelKey: acl.ContentManifestType,
}

// anyUser matches any user at any host.
const anyUser = "*@*"

// DefaultACLs specifies default ACLs.
//
//nolint:gochecknoglobals
var DefaultACLs = []*acl.Entry{
	{
		// everybody can write contents
		User:   anyUser,
		Target: ContentRule,
		Access: acl.AccessLevelAppend,
	},
	{
		// everybody can read global policy.
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:  policy.ManifestType,
			policy.PolicyTypeLabel: policy.PolicyTypeGlobal,
		},
		Access: AccessLevelRead,
	},
	{
		// users *@host can read own host's policy.
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:  policy.ManifestType,
			policy.PolicyTypeLabel: policy.PolicyTypeHost,
			policy.HostnameLabel:   acl.OwnHost,
		},
		Access: AccessLevelRead,
	},
	{
		// username@hostname has full access to their own policies
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey: policy.ManifestType,
			policy.UsernameLabel:  acl.OwnUser,
			policy.HostnameLabel:  acl.OwnHost,
		},
		Access: acl.AccessLevelFull,
	},
	{
		// username@hostname has full access to their own snapshots
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:  snapshot.ManifestType,
			snapshot.UsernameLabel: acl.OwnUser,
			snapshot.HostnameLabel: acl.OwnHost,
		},
		Access: acl.AccessLevelFull,
	},
	{
		// username@hostname has full access to their user account and can change password
		User: anyUser,
		Target: acl.TargetRule{
			manifest.TypeLabelKey:        user.ManifestType,
			user.UsernameAtHostnameLabel: acl.OwnUser + "@" + acl.OwnHost,
		},
		Access: acl.AccessLevelFull,
	},
}

type aclCache struct {
	aclRefreshFrequency time.Duration // +checklocksignore

	mu sync.Mutex
	// +checklocks:mu
	lastRep repo.Repository
	// +checklocks:mu
	nextRefreshTime time.Time
	// +checklocks:mu
	aclEntries []*acl.Entry
}

// Authorize returns authorization info based on ACLs stored in the repository falling back to legacy authorizer
// if no ACL entries are defined.
func (ac *aclCache) Authorize(ctx context.Context, rep repo.Repository, usernameAtHostname string) AuthorizationInfo {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	parts := strings.Split(usernameAtHostname, "@")
	if len(parts) != 2 { //nolint:mnd
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
			log(ctx).Errorf("unable to load aclEntries: %v", err)
		} else {
			ac.aclEntries = newMap
		}
	}

	if len(ac.aclEntries) == 0 {
		return legacyAuthorizationInfo{usernameAtHostname}
	}

	return aclEntriesAuthorizer{acl.EntriesForUser(ac.aclEntries, u, h), u, h}
}

func (ac *aclCache) Refresh(ctx context.Context) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	// ensure ACL entries are reloaded
	ac.nextRefreshTime = time.Time{}

	return nil
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

// DefaultAuthorizer returns Authorizer that will fetch ACLs from the repository
// and evaluate them in the context of current user to determine their permission levels.
// It will fall back to legacy authorizer if no ACL entries are defined in the repository.
func DefaultAuthorizer() Authorizer {
	return &aclCache{
		aclRefreshFrequency: defaultACLRefreshFrequency,
	}
}
