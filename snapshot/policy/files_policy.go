package policy

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules         []string `json:"ignore,omitempty"`
	NoParentIgnoreRules bool     `json:"noParentIgnore,omitempty"`

	DotIgnoreFiles         []string `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool     `json:"noParentDotFiles,omitempty"`

	MaxFileSize int64 `json:"maxFileSize,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *FilesPolicy) Merge(src FilesPolicy) { //nolint:hugeParam
	if p.MaxFileSize == 0 {
		p.MaxFileSize = src.MaxFileSize
	}

	if len(p.IgnoreRules) == 0 {
		p.IgnoreRules = src.IgnoreRules
	}

	if len(p.DotIgnoreFiles) == 0 {
		p.DotIgnoreFiles = src.DotIgnoreFiles
	}
}

// defaultFilesPolicy is the default file ignore policy.
var defaultFilesPolicy = FilesPolicy{
	DotIgnoreFiles: []string{".kopiaignore"},
}
