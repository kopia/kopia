package acl

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/manifest"
)

// placeholders that can be used in ACL definitions to refer to the current user.
const (
	OwnUser = "OWN_USER"
	OwnHost = "OWN_HOST"
)

// TargetRule specifies a list of key and values that must match labels on the target manifest.
// The value can have two special placeholders - OWN_USER and OWN_VALUE representing the matched user
// and host respectively if wildcards are being used.
// Each target rule must have a type "type" key with a value corresponding to a manifest type
// ("snapshot", "policy", "user", "acl"). A special type "content" gives access to contents.
type TargetRule map[string]string

func (r TargetRule) String() string {
	predicates := []string{
		fmt.Sprintf("%v=%v", manifest.TypeLabelKey, r[manifest.TypeLabelKey]),
	}

	for k, v := range r {
		if k != manifest.TypeLabelKey {
			predicates = append(predicates, fmt.Sprintf("%v=%v", k, v))
		}
	}

	return strings.Join(predicates, ",")
}

// matches returns true if a given subject rule matches the given target
// for the provided username & hostname. The rule can use
// OwnUser / OwnHost placeholders.
func (r TargetRule) matches(target map[string]string, username, hostname string) bool {
	for k, v := range r {
		v = strings.ReplaceAll(v, OwnUser, username)
		v = strings.ReplaceAll(v, OwnHost, hostname)

		if target[k] != v {
			return false
		}
	}

	return true
}

// Entry defines access control list entry stored in a manifest which grants the given
// user certain level of access to a target.
type Entry struct {
	ManifestID manifest.ID `json:"-"`
	User       string      `json:"user"`   // supports wildcards such as "*@*", "user@host", "*@host, user@*"
	Target     TargetRule  `json:"target"` // supports OwnUser and OwnHost in labels
	Access     AccessLevel `json:"access,omitempty"`
}

// Validate validates entry.
func (e *Entry) Validate() error {
	if e == nil {
		return errors.Errorf("nil acl")
	}

	parts := strings.Split(e.User, "@")
	if len(parts) != 2 { //nolint:gomnd
		return errors.Errorf("user must be 'username@hostname' possibly including wildcards")
	}

	if e.Target[manifest.TypeLabelKey] == "" {
		return errors.Errorf("ACL target must have a '%v' label", manifest.TypeLabelKey)
	}

	if accessLevelToString[e.Access] == "" {
		return errors.Errorf("valid access level must be specified")
	}

	return nil
}
