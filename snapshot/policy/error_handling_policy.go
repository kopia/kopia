package policy

// ErrorHandlingPolicy controls error hadnling behavior when taking snapshots.
type ErrorHandlingPolicy struct {
	// IgnoreFileErrors controls whether or not snapshot operation should fail when a file throws an error on being read
	IgnoreFileErrors *bool `json:"ignoreFileErrors,omitempty"`

	// IgnoreDirectoryErrors controls whether or not snapshot operation should fail when a directory throws an error on being read or opened
	IgnoreDirectoryErrors *bool `json:"ignoreDirectoryErrors,omitempty"`

	// IgnoreUnknownTypes controls whether or not snapshot operation should fail when it encounters a directory entry of an unknown type.
	IgnoreUnknownTypes *bool `json:"ignoreUnknownTypes,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *ErrorHandlingPolicy) Merge(src ErrorHandlingPolicy) {
	if p.IgnoreFileErrors == nil && src.IgnoreFileErrors != nil {
		p.IgnoreFileErrors = newBool(*src.IgnoreFileErrors)
	}

	if p.IgnoreDirectoryErrors == nil && src.IgnoreDirectoryErrors != nil {
		p.IgnoreDirectoryErrors = newBool(*src.IgnoreDirectoryErrors)
	}

	if p.IgnoreUnknownTypes == nil && src.IgnoreUnknownTypes != nil {
		p.IgnoreUnknownTypes = newBool(*src.IgnoreUnknownTypes)
	}
}

// IgnoreFileErrorsOrDefault returns the ignore-file-error setting if it is set,
// and returns the passed default if not.
func (p *ErrorHandlingPolicy) IgnoreFileErrorsOrDefault(def bool) bool {
	if p.IgnoreFileErrors == nil {
		return def
	}

	return *p.IgnoreFileErrors
}

// IgnoreDirectoryErrorsOrDefault returns the ignore-directory-error setting if it is set,
// and returns the passed default if not.
func (p *ErrorHandlingPolicy) IgnoreDirectoryErrorsOrDefault(def bool) bool {
	if p.IgnoreDirectoryErrors == nil {
		return def
	}

	return *p.IgnoreDirectoryErrors
}

// IgnoreUnknownTypesOrDefault returns the IgnoreUnknownTypes if it is set,
// and returns the passed default if not.
func (p *ErrorHandlingPolicy) IgnoreUnknownTypesOrDefault(def bool) bool {
	if p.IgnoreUnknownTypes == nil {
		return def
	}

	return *p.IgnoreUnknownTypes
}

// defaultErrorHandlingPolicy is the default error handling policy.
var defaultErrorHandlingPolicy = ErrorHandlingPolicy{
	IgnoreFileErrors:      newBool(false),
	IgnoreDirectoryErrors: newBool(false),
	IgnoreUnknownTypes:    newBool(true),
}

func newBool(b bool) *bool {
	return &b
}
