package cli

import (
	"reflect"
	"testing"

	"github.com/kopia/kopia/snapshot/policy"
)

func TestSetErrorHandlingPolicyFromFlags(t *testing.T) {
	initialFileFlagVal := *policyIgnoreFileErrors
	initialDirFlagVal := *policyIgnoreDirectoryErrors

	defer func() {
		*policyIgnoreFileErrors = initialFileFlagVal
		*policyIgnoreDirectoryErrors = initialDirFlagVal
	}()

	for _, tc := range []struct {
		name           string
		startingPolicy *policy.ErrorHandlingPolicy
		fileArg        string
		dirArg         string
		expResult      *policy.ErrorHandlingPolicy
		expErr         bool
		expChangeCount int
	}{
		{
			name: "No values provided as command line arguments",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: true,
			},
			fileArg: "",
			dirArg:  "",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: true,
			},
			expErr:         false,
			expChangeCount: 0,
		},
		{
			name:           "Malformed arguments",
			startingPolicy: &policy.ErrorHandlingPolicy{},
			fileArg:        "not-true-or-false",
			dirArg:         "not-even-inherit",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			expErr:         true,
			expChangeCount: 0,
		},
		{
			name:           "One is malformed, the other well formed",
			startingPolicy: &policy.ErrorHandlingPolicy{},
			fileArg:        "true",
			dirArg:         "some-malformed-arg",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			expErr:         true,
			expChangeCount: 1,
		},
		{
			name: "Inherit case",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrorsSet: true,
			},
			fileArg: "inherit",
			dirArg:  "inherit",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name:           "Set to true",
			startingPolicy: &policy.ErrorHandlingPolicy{},
			fileArg:        "true",
			dirArg:         "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "Set to false",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      true,
				IgnoreDirectoryErrors: true,
			},
			fileArg: "false",
			dirArg:  "false",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File false, dir true",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      true,
				IgnoreDirectoryErrors: true,
			},
			fileArg: "false",
			dirArg:  "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File true, dir false",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      true,
				IgnoreDirectoryErrors: true,
			},
			fileArg: "true",
			dirArg:  "false",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File inherit, dir true",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      true,
				IgnoreDirectoryErrors: true,
			},
			fileArg: "inherit",
			dirArg:  "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File true, dir inherit",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      true,
				IgnoreDirectoryErrors: true,
			},
			fileArg: "true",
			dirArg:  "inherit",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: false,
			},
			expErr:         false,
			expChangeCount: 2,
		},
	} {
		t.Log(tc.name)

		changeCount := 0

		*policyIgnoreFileErrors = tc.fileArg
		*policyIgnoreDirectoryErrors = tc.dirArg

		setErrorHandlingPolicyFromFlags(tc.startingPolicy, &changeCount)

		if !reflect.DeepEqual(tc.startingPolicy, tc.expResult) {
			t.Errorf("Did not get expected output: (actual) %v != %v (expected)", tc.startingPolicy, tc.expResult)
		}
	}
}
