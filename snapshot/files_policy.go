package snapshot

import (
	"path/filepath"

	"github.com/kopia/kopia/fs"
)

// FilesPolicy describes files to be uploaded when taking snapshots
type FilesPolicy struct {
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
	MaxSize *int     `json:"maxSize,omitempty"`
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

func fileNameMatches(fname string, pattern string) bool {
	ok, err := filepath.Match(pattern, fname)
	if err != nil {
		log.Warningf("%v, assuming %q does not match the pattern", err, fname)
		return false
	}

	return ok
}

func fileNameMatchesAnyPattern(e *fs.EntryMetadata, patterns []string) bool {
	for _, i := range patterns {
		if fileNameMatches(e.Name, i) {
			return true
		}
	}

	return false
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

var defaultFilesPolicy = &FilesPolicy{}
