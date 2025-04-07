package acl

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// ContentManifestType is a type that can be used in TargetRule to specify
// access level to contents as opposed to metadata.
const ContentManifestType = "content"

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

type valueValidatorFunc func(v string) error

func nonEmptyString(v string) error {
	if v == "" {
		return errors.New("must be non-empty")
	}

	return nil
}

func oneOf(allowed ...string) valueValidatorFunc {
	return func(v string) error {
		for _, a := range allowed {
			if v == a {
				return nil
			}
		}

		return errors.Errorf("must be one of: %v", strings.Join(allowed, ", "))
	}
}

//nolint:gochecknoglobals
var allowedLabelsForType = map[string]map[string]valueValidatorFunc{
	ContentManifestType: {},
	policy.ManifestType: {
		policy.HostnameLabel: nonEmptyString,
		policy.UsernameLabel: nonEmptyString,
		policy.PathLabel:     nonEmptyString,
		policy.PolicyTypeLabel: oneOf(
			policy.PolicyTypeGlobal,
			policy.PolicyTypeHost,
			policy.PolicyTypeUser,
			policy.PolicyTypePath,
		),
	},
	snapshot.ManifestType: {
		snapshot.HostnameLabel: nonEmptyString,
		snapshot.UsernameLabel: nonEmptyString,
		snapshot.PathLabel:     nonEmptyString,
	},
	user.ManifestType: {
		user.UsernameAtHostnameLabel: nonEmptyString,
	},
	aclManifestType: {},
}

// Validate validates entry.
func (e *Entry) Validate() error {
	if e == nil {
		return errors.New("nil acl")
	}

	parts := strings.Split(e.User, "@")
	if len(parts) != 2 { //nolint:mnd
		return errors.New("user must be 'username@hostname' possibly including wildcards")
	}

	typ := e.Target[manifest.TypeLabelKey]
	if typ == "" {
		return errors.Errorf("ACL target must have a '%v' label", manifest.TypeLabelKey)
	}

	allowedLabels, ok := allowedLabelsForType[typ]
	if !ok {
		return errors.Errorf("invalid '%v' label, must be one of: %v", manifest.TypeLabelKey, strings.Join(allowedTypeNames(), ", "))
	}

	for k, v := range e.Target {
		if k == manifest.TypeLabelKey {
			continue
		}

		val := allowedLabels[k]
		if val == nil {
			return errors.Errorf("unsupported label '%v' for type '%v', must be one of: %v", k, typ, strings.Join(allowedLabelNames(allowedLabels), ", "))
		}

		if err := val(v); err != nil {
			return errors.Errorf("invalid label '%v=%v' for type '%v': %v", k, v, typ, err)
		}
	}

	if accessLevelToString[e.Access] == "" {
		return errors.New("valid access level must be specified")
	}

	return nil
}

func allowedTypeNames() []string {
	var result []string

	for k := range allowedLabelsForType {
		result = append(result, k)
	}

	sort.Strings(result)

	return result
}

func allowedLabelNames(m map[string]valueValidatorFunc) []string {
	var result []string

	for k := range m {
		result = append(result, k)
	}

	sort.Strings(result)

	return result
}
