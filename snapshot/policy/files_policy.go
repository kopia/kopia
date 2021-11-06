package policy

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules         []string `json:"ignore,omitempty"`
	NoParentIgnoreRules bool     `json:"noParentIgnore,omitempty"`

	DotIgnoreFiles         []string `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool     `json:"noParentDotFiles,omitempty"`

	IgnoreCacheDirectories *OptionalBool `json:"ignoreCacheDirs,omitempty"`

	MaxFileSize int64 `json:"maxFileSize,omitempty"`

	OneFileSystem *OptionalBool `json:"oneFileSystem,omitempty"`
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

	if p.IgnoreCacheDirectories == nil {
		p.IgnoreCacheDirectories = src.IgnoreCacheDirectories
	}

	if p.OneFileSystem == nil {
		p.OneFileSystem = src.OneFileSystem
	}
}

// defaultFilesPolicy is the default file ignore policy.
var defaultFilesPolicy = FilesPolicy{
	DotIgnoreFiles: []string{".kopiaignore"},
}
