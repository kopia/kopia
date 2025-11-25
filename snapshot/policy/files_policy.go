package policy

import "github.com/kopia/kopia/snapshot"

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules            []string      `json:"ignore,omitempty"`
	NoParentIgnoreRules    bool          `json:"noParentIgnore,omitempty"`
	DotIgnoreFiles         []string      `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool          `json:"noParentDotFiles,omitempty"`
	IgnoreCacheDirectories *OptionalBool `json:"ignoreCacheDirs,omitempty"`
	MaxFileSize            int64         `json:"maxFileSize,omitempty"`
	OneFileSystem          *OptionalBool `json:"oneFileSystem,omitempty"`
}

// FilesPolicyDefinition specifies which policy definition provided the value of a particular field.
type FilesPolicyDefinition struct {
	IgnoreRules            snapshot.SourceInfo `json:"ignore,omitempty"`
	NoParentIgnoreRules    snapshot.SourceInfo `json:"noParentIgnore,omitempty"`
	DotIgnoreFiles         snapshot.SourceInfo `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles snapshot.SourceInfo `json:"noParentDotFiles,omitempty"`
	IgnoreCacheDirectories snapshot.SourceInfo `json:"ignoreCacheDirs,omitempty"`
	MaxFileSize            snapshot.SourceInfo `json:"maxFileSize,omitempty"`
	OneFileSystem          snapshot.SourceInfo `json:"oneFileSystem,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *FilesPolicy) Merge(src FilesPolicy, def *FilesPolicyDefinition, si snapshot.SourceInfo) {
	mergeStringList(&p.IgnoreRules, src.IgnoreRules, &def.IgnoreRules, si)
	mergeBool(&p.NoParentIgnoreRules, src.NoParentIgnoreRules, &def.NoParentIgnoreRules, si)
	mergeStringsReplace(&p.DotIgnoreFiles, src.DotIgnoreFiles, &def.DotIgnoreFiles, si)
	mergeBool(&p.NoParentDotIgnoreFiles, src.NoParentDotIgnoreFiles, &def.NoParentDotIgnoreFiles, si)
	mergeOptionalBool(&p.IgnoreCacheDirectories, src.IgnoreCacheDirectories, &def.IgnoreCacheDirectories, si)
	mergeInt64(&p.MaxFileSize, src.MaxFileSize, &def.MaxFileSize, si)
	mergeOptionalBool(&p.OneFileSystem, src.OneFileSystem, &def.OneFileSystem, si)
}
