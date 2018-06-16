package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/rs/zerolog/log"
)

// ErrPolicyNotFound is returned when the policy is not found.
var ErrPolicyNotFound = errors.New("policy not found")

// Policy describes snapshot policy for a single source.
type Policy struct {
	Labels           map[string]string `json:"-"`
	RetentionPolicy  RetentionPolicy   `json:"retention"`
	FilesPolicy      FilesPolicy       `json:"files"`
	SchedulingPolicy SchedulingPolicy  `json:"scheduling"`
	NoParent         bool              `json:"noParent,omitempty"`
}

func (p *Policy) String() string {
	var buf bytes.Buffer

	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")
	if err := e.Encode(p); err != nil {
		log.Warn().Err(err).Msg("unable to policy as JSON")
	}
	return buf.String()
}

// ID returns globally unique identifier of the policy.
func (p *Policy) ID() string {
	return p.Labels["id"]
}

// Target returns the SourceInfo describing username, host and path targeted by the policy.
func (p *Policy) Target() SourceInfo {
	return SourceInfo{
		Host:     p.Labels["hostname"],
		UserName: p.Labels["username"],
		Path:     p.Labels["path"],
	}
}

// MergePolicies computes the policy by applying the specified list of policies in order.
func MergePolicies(policies []*Policy) *Policy {
	var merged Policy

	for _, p := range policies {
		if p.NoParent {
			return &merged
		}

		mergeRetentionPolicy(&merged.RetentionPolicy, &p.RetentionPolicy)
		mergeFilesPolicy(&merged.FilesPolicy, &p.FilesPolicy)
		mergeSchedulingPolicy(&merged.SchedulingPolicy, &p.SchedulingPolicy)
	}

	// Merge default expiration policy.
	mergeRetentionPolicy(&merged.RetentionPolicy, defaultRetentionPolicy)
	mergeFilesPolicy(&merged.FilesPolicy, defaultFilesPolicy)
	mergeSchedulingPolicy(&merged.SchedulingPolicy, defaultSchedulingPolicy)

	return &merged
}

func intPtr(n int) *int {
	return &n
}
