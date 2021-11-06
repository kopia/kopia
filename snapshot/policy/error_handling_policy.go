package policy

// ErrorHandlingPolicy controls error hadnling behavior when taking snapshots.
type ErrorHandlingPolicy struct {
	// IgnoreFileErrors controls whether or not snapshot operation should fail when a file throws an error on being read
	IgnoreFileErrors *OptionalBool `json:"ignoreFileErrors,omitempty"`

	// IgnoreDirectoryErrors controls whether or not snapshot operation should fail when a directory throws an error on being read or opened
	IgnoreDirectoryErrors *OptionalBool `json:"ignoreDirectoryErrors,omitempty"`

	// IgnoreUnknownTypes controls whether or not snapshot operation should fail when it encounters a directory entry of an unknown type.
	IgnoreUnknownTypes *OptionalBool `json:"ignoreUnknownTypes,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *ErrorHandlingPolicy) Merge(src ErrorHandlingPolicy) {
	mergeOptionalBool(&p.IgnoreFileErrors, src.IgnoreFileErrors)
	mergeOptionalBool(&p.IgnoreDirectoryErrors, src.IgnoreDirectoryErrors)
	mergeOptionalBool(&p.IgnoreUnknownTypes, src.IgnoreUnknownTypes)
}

// defaultErrorHandlingPolicy is the default error handling policy.
var defaultErrorHandlingPolicy = ErrorHandlingPolicy{
	IgnoreFileErrors:      newOptionalBool(false),
	IgnoreDirectoryErrors: newOptionalBool(false),
	IgnoreUnknownTypes:    newOptionalBool(true),
}
