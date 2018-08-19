package ignorefs

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules         []string `json:"ignore,omitempty"`
	NoParentIgnoreRules bool     `json:"noParentIgnoreRules,omitempty"`

	DotIgnoreFiles         []string `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool     `json:"noParentDotFiles,omitempty"`

	MaxFileSize int64 `json:"maxFileSize,omitempty"`
}

// Merge applies default values from the provided policy.
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
}

// DefaultFilesPolicy is the default file ignore policy.
var DefaultFilesPolicy = FilesPolicy{
	DotIgnoreFiles: []string{".kopiaignore"},
}
