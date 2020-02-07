package policy

// ErrorHandlingPolicy controls error hadnling behavior when taking snapshots.
type ErrorHandlingPolicy struct {
	// IgnoreFileErrors controls whether or not snapshot operation should terminate when a file throws an error on being read
	IgnoreFileErrors bool `json:"ignoreFileErrs,omitempty"`

	// IgnoreFileErrorSet denotes whether the IgnoreFileErrors bool was actually set in this policy, or if it should be inherited
	IgnoreFileErrorsSet bool `json:"ignoreFileErrsSet,omitempty"`

	// IgnoreDirectoryErrors controls whether or not snapshot operation should terminate when a directory throws an error on being read or opened
	IgnoreDirectoryErrors bool `json:"ignoreDirErrs,omitempty"`

	// IgnoreDirectoryErrorsSet denotes whether the IgnoreDirectoryErrors bool was actually set in this policy, or if it should be inherited
	IgnoreDirectoryErrorsSet bool `json:"ignoreDirErrsSet,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *ErrorHandlingPolicy) Merge(src ErrorHandlingPolicy) {
	if !p.IgnoreFileErrorsSet && src.IgnoreFileErrorsSet {
		p.IgnoreFileErrors = src.IgnoreFileErrors
		p.IgnoreFileErrorsSet = true
	}

	if !p.IgnoreDirectoryErrorsSet && src.IgnoreDirectoryErrorsSet {
		p.IgnoreDirectoryErrors = src.IgnoreDirectoryErrors
		p.IgnoreDirectoryErrorsSet = true
	}
}

// defaultErrorHandlingPolicy is the default error handling policy.
var defaultErrorHandlingPolicy = ErrorHandlingPolicy{
	IgnoreFileErrors:         false,
	IgnoreFileErrorsSet:      true,
	IgnoreDirectoryErrors:    false,
	IgnoreDirectoryErrorsSet: true,
}
