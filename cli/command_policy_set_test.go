package cli

import (
	"reflect"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/snapshot/policy"
)

func TestSetErrorHandlingPolicyFromFlags(t *testing.T) {
	var pef policyErrorFlags

	ctx := testlogging.Context(t)

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
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			fileArg: "",
			dirArg:  "",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
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
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
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
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: nil,
			},
			expErr:         true,
			expChangeCount: 1,
		},
		{
			name:           "Inherit case",
			startingPolicy: &policy.ErrorHandlingPolicy{},
			fileArg:        "inherit",
			dirArg:         "inherit",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: nil,
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
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "Set to false",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			fileArg: "false",
			dirArg:  "false",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(false),
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File false, dir true",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(false),
			},
			fileArg: "false",
			dirArg:  "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File true, dir false",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			fileArg: "true",
			dirArg:  "false",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(false),
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File inherit, dir true",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: newOptionalBool(false),
			},
			fileArg: "inherit",
			dirArg:  "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "File true, dir inherit",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(false),
				IgnoreDirectoryErrors: newOptionalBool(true),
			},
			fileArg: "true",
			dirArg:  "inherit",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      newOptionalBool(true),
				IgnoreDirectoryErrors: nil,
			},
			expErr:         false,
			expChangeCount: 2,
		},
	} {
		t.Log(tc.name)

		changeCount := 0

		pef.policyIgnoreFileErrors = tc.fileArg
		pef.policyIgnoreDirectoryErrors = tc.dirArg

		pef.setErrorHandlingPolicyFromFlags(ctx, tc.startingPolicy, &changeCount)

		if !reflect.DeepEqual(tc.startingPolicy, tc.expResult) {
			t.Errorf("Did not get expected output: (actual) %v != %v (expected)", tc.startingPolicy, tc.expResult)
		}
	}
}

func TestSetSchedulingPolicyFromFlags(t *testing.T) {
	var psf policySchedulingFlags

	ctx := testlogging.Context(t)

	for _, tc := range []struct {
		name           string
		startingPolicy *policy.SchedulingPolicy
		intervalArg    []time.Duration
		timesOfDayArg  []string
		manualArg      bool
		expResult      *policy.SchedulingPolicy
		expErr         bool
		expChangeCount int
	}{
		{
			name:           "No flags provided, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			expResult:      &policy.SchedulingPolicy{},
			expErr:         false,
			expChangeCount: 0,
		},
		{
			name:           "Manual flag set to true, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			manualArg:      true,
			expResult: &policy.SchedulingPolicy{
				Manual: true,
			},
			expErr:         false,
			expChangeCount: 1,
		},
		{
			name:           "Interval set, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			intervalArg: []time.Duration{
				time.Hour * 1,
			},
			expResult: &policy.SchedulingPolicy{
				IntervalSeconds: 3600,
			},
			expErr:         false,
			expChangeCount: 1,
		},
		{
			name:           "Times of day set, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			timesOfDayArg: []string{
				"12:00",
			},
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{
					{
						Hour:   12,
						Minute: 0,
					},
				},
			},
			expErr:         false,
			expChangeCount: 1,
		},
		{
			name:           "Manual and interval set, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			intervalArg: []time.Duration{
				time.Hour * 1,
			},
			manualArg:      true,
			expResult:      &policy.SchedulingPolicy{},
			expErr:         true,
			expChangeCount: 0,
		},
		{
			name:           "Manual and times of day set, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			timesOfDayArg: []string{
				"12:00",
			},
			manualArg:      true,
			expResult:      &policy.SchedulingPolicy{},
			expErr:         true,
			expChangeCount: 0,
		},
		{
			name: "Manual set to true, starting policy with interval",
			startingPolicy: &policy.SchedulingPolicy{
				IntervalSeconds: 3600,
			},
			manualArg: true,
			expResult: &policy.SchedulingPolicy{
				Manual: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "Manual set to true, starting policy with times of day",
			startingPolicy: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{
					{
						Hour:   12,
						Minute: 0,
					},
				},
			},
			manualArg: true,
			expResult: &policy.SchedulingPolicy{
				Manual: true,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "Manual set to true, starting policy with schedule",
			startingPolicy: &policy.SchedulingPolicy{
				IntervalSeconds: 3600,
				TimesOfDay: []policy.TimeOfDay{
					{
						Hour:   12,
						Minute: 0,
					},
				},
			},
			manualArg: true,
			expResult: &policy.SchedulingPolicy{
				Manual: true,
			},
			expErr:         false,
			expChangeCount: 3,
		},
		{
			name: "Interval set, starting policy with manual",
			startingPolicy: &policy.SchedulingPolicy{
				Manual: true,
			},
			intervalArg: []time.Duration{
				time.Hour * 1,
			},
			expResult: &policy.SchedulingPolicy{
				IntervalSeconds: 3600,
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "Times of day set, starting policy with manual",
			startingPolicy: &policy.SchedulingPolicy{
				Manual: true,
			},
			timesOfDayArg: []string{
				"12:00",
			},
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{
					{
						Hour:   12,
						Minute: 0,
					},
				},
			},
			expErr:         false,
			expChangeCount: 2,
		},
		{
			name: "Change time of day",
			startingPolicy: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
			},
			timesOfDayArg: []string{"13:00,14:00"},
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{
					{Hour: 13, Minute: 0},
					{Hour: 14, Minute: 0},
				},
			},
			expErr:         false,
			expChangeCount: 1,
		},
		{
			name: "Remove time of day",
			startingPolicy: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
			},
			timesOfDayArg: []string{"inherit"},
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: nil,
			},
			expErr:         false,
			expChangeCount: 1,
		},
	} {
		t.Log(tc.name)

		changeCount := 0

		psf.policySetInterval = tc.intervalArg
		psf.policySetTimesOfDay = tc.timesOfDayArg
		psf.policySetManual = tc.manualArg

		err := psf.setSchedulingPolicyFromFlags(ctx, tc.startingPolicy, &changeCount)
		if tc.expErr {
			if err == nil {
				t.Errorf("Expected error but got none")
			}
		} else {
			if err != nil {
				t.Errorf("Expected none but got err: %v", err)
			}
		}

		if !reflect.DeepEqual(tc.startingPolicy, tc.expResult) {
			t.Errorf("Did not get expected output: (actual) %v != %v (expected)", tc.startingPolicy, tc.expResult)
		}

		if !reflect.DeepEqual(tc.expChangeCount, changeCount) {
			t.Errorf("Did not get expected output: (actual) %v != %v (expected)", tc.expChangeCount, changeCount)
		}
	}
}

func newOptionalBool(b policy.OptionalBool) *policy.OptionalBool {
	return &b
}
