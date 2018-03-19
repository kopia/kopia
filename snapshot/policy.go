package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/fs"
)

// ErrPolicyNotFound is returned when the policy is not found.
var ErrPolicyNotFound = errors.New("policy not found")

// RetentionPolicy describes snapshot retention policy.
type RetentionPolicy struct {
	KeepLatest  *int `json:"keepLatest,omitempty"`
	KeepHourly  *int `json:"keepHourly,omitempty"`
	KeepDaily   *int `json:"keepDaily,omitempty"`
	KeepWeekly  *int `json:"keepWeekly,omitempty"`
	KeepMonthly *int `json:"keepMonthly,omitempty"`
	KeepAnnual  *int `json:"keepAnnual,omitempty"`
}

var defaultRetentionPolicy = &RetentionPolicy{
	KeepLatest:  intPtr(1),
	KeepHourly:  intPtr(48),
	KeepDaily:   intPtr(7),
	KeepWeekly:  intPtr(4),
	KeepMonthly: intPtr(4),
	KeepAnnual:  intPtr(0),
}

// FilesPolicy describes files to be uploaded when taking snapshots
type FilesPolicy struct {
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
	MaxSize *int     `json:"maxSize,omitempty"`
}

// SchedulingPolicy describes policy for scheduling snapshots.
type SchedulingPolicy struct {
	Frequency time.Duration `json:"frequency"`
}

// ShouldInclude determines whether given filesystem entry should be included based on the policy.
func (p *FilesPolicy) ShouldInclude(e *fs.EntryMetadata) bool {
	if len(p.Include) > 0 && !fileNameMatchesAnyPattern(e, p.Include) {
		return false
	}

	if len(p.Exclude) > 0 && fileNameMatchesAnyPattern(e, p.Include) {
		return false
	}

	if p.MaxSize != nil && e.Type == fs.EntryTypeFile && e.FileSize > int64(*p.MaxSize) {
		return false
	}

	return true
}

func fileNameMatchesAnyPattern(e *fs.EntryMetadata, patterns []string) bool {
	for _, i := range patterns {
		if fileNameMatches(e.Name, i) {
			return true
		}
	}

	return false
}

var defaultFilesPolicy = &FilesPolicy{}

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

func fileNameMatches(fname string, pattern string) bool {
	ok, err := filepath.Match(pattern, fname)
	if err != nil {
		log.Printf("warning: %v, assuming %q does not match the pattern", err, fname)
		return false
	}

	return ok
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
	}

	// Merge default expiration policy.
	mergeRetentionPolicy(&merged.RetentionPolicy, defaultRetentionPolicy)
	mergeFilesPolicy(&merged.FilesPolicy, defaultFilesPolicy)

	return &merged
}

func mergeRetentionPolicy(dst, src *RetentionPolicy) {
	if dst.KeepLatest == nil {
		dst.KeepLatest = src.KeepLatest
	}
	if dst.KeepHourly == nil {
		dst.KeepHourly = src.KeepHourly
	}
	if dst.KeepDaily == nil {
		dst.KeepDaily = src.KeepDaily
	}
	if dst.KeepWeekly == nil {
		dst.KeepWeekly = src.KeepWeekly
	}
	if dst.KeepMonthly == nil {
		dst.KeepMonthly = src.KeepMonthly
	}
	if dst.KeepAnnual == nil {
		dst.KeepAnnual = src.KeepAnnual
	}
}

func mergeFilesPolicy(dst, src *FilesPolicy) {
	if dst.MaxSize == nil {
		dst.MaxSize = src.MaxSize
	}

	if len(dst.Include) == 0 {
		dst.Include = src.Include
	}

	if len(dst.Exclude) == 0 {
		dst.Exclude = src.Exclude
	}
}

func intPtr(n int) *int {
	return &n
}
