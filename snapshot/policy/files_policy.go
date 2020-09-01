package policy

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules         []string `json:"ignore,omitempty"`
	NoParentIgnoreRules bool     `json:"noParentIgnore,omitempty"`

	DotIgnoreFiles         []string `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool     `json:"noParentDotFiles,omitempty"`

	IgnoreCacheDirs *bool `json:"ignoreCacheDirs,omitempty"`

	MaxFileSize int64 `json:"maxFileSize,omitempty"`
}

// Merge applies default values from the provided policy.
// nolint:gocritic
func (p *FilesPolicy) Merge(src FilesPolicy) {
	if p.MaxFileSize == 0 {
		p.MaxFileSize = src.MaxFileSize
	}

	if len(p.IgnoreRules) == 0 {
		p.IgnoreRules = src.IgnoreRules
	}

	if len(p.DotIgnoreFiles) == 0 {
		p.DotIgnoreFiles = src.DotIgnoreFiles
	}

	if p.IgnoreCacheDirs == nil {
		p.IgnoreCacheDirs = src.IgnoreCacheDirs
	}
}

// IgnoreCacheDirectoriesOrDefault gets the value of IgnoreCacheDirs or the provided default if not set.
func (p *FilesPolicy) IgnoreCacheDirectoriesOrDefault(def bool) bool {
	if p.IgnoreCacheDirs == nil {
		return def
	}

	return *p.IgnoreCacheDirs
}

// defaultFilesPolicy is the default file ignore policy.
var defaultFilesPolicy = FilesPolicy{
	DotIgnoreFiles: []string{".kopiaignore"},
}
