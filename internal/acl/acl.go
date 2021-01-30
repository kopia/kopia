package acl

import "github.com/kopia/kopia/repo/manifest"

// placeholders that can be used in ACL definitions to refer to the current user.
const (
	CurrentUsernamePlaceholder = "CURRENT_USER"
	CurrentHostnamePlaceholder = "CURRENT_HOST"
)

// AccessControlList defines access control list.
type AccessControlList struct {
	ManifestID manifest.ID `json:"-"`

	ContentAccess  AccessLevel                     `json:"content,omitempty"`
	ManifestAccess map[string][]ManifestAccessRule `json:"manifest"`
}

// ContentAccessLevel implements authz.AuthorizationInfo.
func (m AccessControlList) ContentAccessLevel() AccessLevel {
	if m.ContentAccess == AccessLevelUnspecified {
		return AccessLevelNone
	}

	return m.ContentAccess
}

// ManifestAccessLevel implements authz.AuthorizationInfo.
func (m AccessControlList) ManifestAccessLevel(labels map[string]string) AccessLevel {
	result := AccessLevelUnspecified

	for _, r := range m.ManifestAccess[labels[manifest.TypeLabelKey]] {
		if r.Access != AccessLevelUnspecified && isMatch(labels, r.Match) {
			result = r.Access
		}
	}

	if result == AccessLevelUnspecified {
		result = AccessLevelNone
	}

	return result
}

// Personalize returns modified access control list by replacing username and hostname
// placeholders with the provided username and hostname.
func (m AccessControlList) Personalize(username, hostname string) AccessControlList {
	result := AccessControlList{
		ContentAccess:  m.ContentAccess,
		ManifestAccess: map[string][]ManifestAccessRule{},
	}

	// combine manifest access rules left-to-right
	for k, v := range m.ManifestAccess {
		for _, r := range v {
			result.ManifestAccess[k] = append(result.ManifestAccess[k], r.Personalize(username, hostname))
		}
	}

	return result
}

func isMatch(actual, expected map[string]string) bool {
	for k, v := range expected {
		if actual[k] != v {
			return false
		}
	}

	return true
}

// AccessLevel specifies access level.
type AccessLevel int

// Supported access levels.
const (
	AccessLevelUnspecified AccessLevel = 0 // permissions unspecified
	AccessLevelNone        AccessLevel = 1 // no access
	AccessLevelView        AccessLevel = 2 // permissions to view, but not change
	AccessLevelAppend      AccessLevel = 3 // permissions to view/add but not update/delete.
	AccessLevelFull        AccessLevel = 4 // permission to view/add/update/delete.
)

// ManifestAccessRule specifies rules for accessing policies and snapshot manifests.
type ManifestAccessRule struct {
	Access AccessLevel       `json:"level"`
	Match  map[string]string `json:"match"`
}

// Personalize returns personalized version of the rule by replacing current username/hostname
// placeholders with the provided values.
func (m ManifestAccessRule) Personalize(username, hostname string) ManifestAccessRule {
	result := ManifestAccessRule{
		Access: m.Access,
		Match:  map[string]string{},
	}

	for k, v := range m.Match {
		switch v {
		case CurrentUsernamePlaceholder:
			result.Match[k] = username

		case CurrentHostnamePlaceholder:
			result.Match[k] = hostname
		default:
			result.Match[k] = v
		}
	}

	return result
}

// Merge returns a merged AccessControlList from a provided list,
// which must be ordered from most general (global) to most specific (individual user) ACLs.
func Merge(acls ...AccessControlList) AccessControlList {
	result := AccessControlList{
		ManifestAccess: map[string][]ManifestAccessRule{},
	}

	for _, a := range acls {
		// when merging, pick the most specific content access.
		// this allows both allow and deny rules.
		if a.ContentAccess != AccessLevelUnspecified {
			result.ContentAccess = a.ContentAccess
		}

		// combine manifest access rules left-to-right
		for k, v := range a.ManifestAccess {
			result.ManifestAccess[k] = append(result.ManifestAccess[k], v...)
		}
	}

	return result
}
