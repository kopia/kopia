package policy

import "github.com/kopia/kopia/snapshot"

// FilesPolicy describes files to be ignored when taking snapshots.
type FilesPolicy struct {
	IgnoreRules            []string      `json:"ignore,omitempty"`
	NoParentIgnoreRules    bool          `json:"noParentIgnore,omitzero"`
	DotIgnoreFiles         []string      `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles bool          `json:"noParentDotFiles,omitzero"`
	IgnoreCacheDirectories *OptionalBool `json:"ignoreCacheDirs,omitzero"`
	MaxFileSize            int64         `json:"maxFileSize,omitzero"`
	OneFileSystem          *OptionalBool `json:"oneFileSystem,omitzero"`
}

// FilesPolicyDefinition specifies which policy definition provided the value of a particular field.
type FilesPolicyDefinition struct {
	IgnoreRules            snapshot.SourceInfo `json:"ignore,omitempty"`
	NoParentIgnoreRules    snapshot.SourceInfo `json:"noParentIgnore,omitzero"`
	DotIgnoreFiles         snapshot.SourceInfo `json:"ignoreDotFiles,omitempty"`
	NoParentDotIgnoreFiles snapshot.SourceInfo `json:"noParentDotFiles,omitzero"`
	IgnoreCacheDirectories snapshot.SourceInfo `json:"ignoreCacheDirs,omitzero"`
	MaxFileSize            snapshot.SourceInfo `json:"maxFileSize,omitzero"`
	OneFileSystem          snapshot.SourceInfo `json:"oneFileSystem,omitzero"`
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
