package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"path/filepath"

	"github.com/kopia/kopia/fs"
)

// ErrPolicyNotFound is returned when the policy is not found.
var ErrPolicyNotFound = errors.New("policy not found")

// ExpirationPolicy describes snapshot expiration policy.
type ExpirationPolicy struct {
	KeepLatest  *int `json:"keepLatest,omitempty"`
	KeepHourly  *int `json:"keepHourly,omitempty"`
	KeepDaily   *int `json:"keepDaily,omitempty"`
	KeepWeekly  *int `json:"keepWeekly,omitempty"`
	KeepMonthly *int `json:"keepMonthly,omitempty"`
	KeepAnnual  *int `json:"keepAnnual,omitempty"`
}

var defaultExpirationPolicy = &ExpirationPolicy{
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

// ShouldInclude determines whether given filesystem entry should be included based on the policy.
func (p *FilesPolicy) ShouldInclude(e *fs.EntryMetadata) bool {
	if len(p.Include) > 0 {
		include := false
		for _, i := range p.Include {
			if fileNameMatches(e.Name, i) {
				include = true
				break
			}
		}
		if !include {
			// have include rules, but none of them matched
			return false
		}
	}

	if len(p.Exclude) > 0 {
		for _, ex := range p.Exclude {
			if fileNameMatches(e.Name, ex) {
				return false
			}
		}
	}

	if p.MaxSize != nil && e.Type == fs.EntryTypeFile && e.FileSize > int64(*p.MaxSize) {
		return false
	}

	return true
}

var defaultFilesPolicy = &FilesPolicy{}

// Policy describes snapshot policy for a single source.
type Policy struct {
	Labels           map[string]string `json:"-"`
	ExpirationPolicy ExpirationPolicy  `json:"expiration"`
	FilesPolicy      FilesPolicy       `json:"files"`
	NoParent         bool              `json:"noParent,omitempty"`
}

func (p *Policy) String() string {
	var buf bytes.Buffer

	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")
	e.Encode(p)
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

func MergePolicies(policies []*Policy) *Policy {
	var merged Policy

	for _, p := range policies {
		if p.NoParent {
			break
		}

		mergeExpirationPolicy(&merged.ExpirationPolicy, &p.ExpirationPolicy)
		mergeFilesPolicy(&merged.FilesPolicy, &p.FilesPolicy)
	}

	// Merge default expiration policy.
	mergeExpirationPolicy(&merged.ExpirationPolicy, defaultExpirationPolicy)
	mergeFilesPolicy(&merged.FilesPolicy, defaultFilesPolicy)

	return &merged
}

func mergeExpirationPolicy(dst, src *ExpirationPolicy) {
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
