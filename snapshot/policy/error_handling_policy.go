package policy

import "github.com/kopia/kopia/snapshot"

// ErrorHandlingPolicy controls error handling behavior when taking snapshots.
type ErrorHandlingPolicy struct {
	// IgnoreFileErrors controls whether or not snapshot operation should fail when a file throws an error on being read
	IgnoreFileErrors *OptionalBool `json:"ignoreFileErrors,omitzero"`

	// IgnoreDirectoryErrors controls whether or not snapshot operation should fail when a directory throws an error on being read or opened
	IgnoreDirectoryErrors *OptionalBool `json:"ignoreDirectoryErrors,omitzero"`

	// IgnoreUnknownTypes controls whether or not snapshot operation should fail when it encounters a directory entry of an unknown type.
	IgnoreUnknownTypes *OptionalBool `json:"ignoreUnknownTypes,omitzero"`
}

// ErrorHandlingPolicyDefinition specifies which policy definition provided the value of a particular field.
type ErrorHandlingPolicyDefinition struct {
	IgnoreFileErrors      snapshot.SourceInfo `json:"ignoreFileErrors,omitzero"`
	IgnoreDirectoryErrors snapshot.SourceInfo `json:"ignoreDirectoryErrors,omitzero"`
	IgnoreUnknownTypes    snapshot.SourceInfo `json:"ignoreUnknownTypes,omitzero"`
}

// Merge applies default values from the provided policy.
func (p *ErrorHandlingPolicy) Merge(src ErrorHandlingPolicy, def *ErrorHandlingPolicyDefinition, si snapshot.SourceInfo) {
	mergeOptionalBool(&p.IgnoreFileErrors, src.IgnoreFileErrors, &def.IgnoreFileErrors, si)
	mergeOptionalBool(&p.IgnoreDirectoryErrors, src.IgnoreDirectoryErrors, &def.IgnoreDirectoryErrors, si)
	mergeOptionalBool(&p.IgnoreUnknownTypes, src.IgnoreUnknownTypes, &def.IgnoreUnknownTypes, si)
}
