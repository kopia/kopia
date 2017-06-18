package snapshot

import (
	"bytes"
	"encoding/json"
)

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
}

// FilesPolicy describes files to be uploaded when taking snapshots
type FilesPolicy struct {
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
	MaxSize *int     `json:"maxSize,omitempty"`
}

var defaultFilesPolicy = &FilesPolicy{}

// Policy describes snapshot policy for a single source.
type Policy struct {
	Source     SourceInfo       `json:"source"`
	Expiration ExpirationPolicy `json:"expiration"`
	Files      FilesPolicy      `json:"files"`
	NoParent   bool             `json:"noParent,omitempty"`
}

func (p *Policy) String() string {
	var buf bytes.Buffer

	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")
	e.Encode(p)
	return buf.String()
}

func mergePolicies(policies []*Policy) *Policy {
	var merged Policy

	for _, p := range policies {
		if p.NoParent {
			break
		}

		mergeExpirationPolicy(&merged.Expiration, &p.Expiration)
		mergeFilesPolicy(&merged.Files, &p.Files)
	}

	// Merge default expiration policy.
	mergeExpirationPolicy(&merged.Expiration, defaultExpirationPolicy)
	mergeFilesPolicy(&merged.Files, defaultFilesPolicy)

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
