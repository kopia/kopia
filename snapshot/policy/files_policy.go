package policy

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules         []string `json:"ignore,omitempty"`
	NoParentIgnoreRules bool     `json:"noParentIgnore,omitempty"`

	DotIgnoreFiles         []string `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool     `json:"noParentDotFiles,omitempty"`

	IgnoreCacheDirs *bool `json:"ignoreCacheDirs,omitempty"`

	MaxFileSize int64 `json:"maxFileSize,omitempty"`

	OneFileSystem *bool `json:"oneFileSystem,omitempty"`
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

	if p.IgnoreCacheDirs == nil {
		p.IgnoreCacheDirs = src.IgnoreCacheDirs
	}

	if p.OneFileSystem == nil {
		p.OneFileSystem = src.OneFileSystem
	}
}

// IgnoreCacheDirectoriesOrDefault gets the value of IgnoreCacheDirs or the provided default if not set.
func (p *FilesPolicy) IgnoreCacheDirectoriesOrDefault(def bool) bool {
	if p.IgnoreCacheDirs == nil {
		return def
	}

	return *p.IgnoreCacheDirs
}

// OneFileSystemOrDefault gets the value of OneFileSystem or the provided default if not set.
func (p *FilesPolicy) OneFileSystemOrDefault(def bool) bool {
	if p.OneFileSystem == nil {
		return def
	}

	return *p.OneFileSystem
}

// defaultFilesPolicy is the default file ignore policy.
var defaultFilesPolicy = FilesPolicy{
	DotIgnoreFiles: []string{".kopiaignore"},
}
