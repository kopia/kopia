package cli

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			fileArg: "",
			dirArg:  "",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
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
				IgnoreFileErrors:      policy.NewOptionalBool(true),
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
			expChangeCount: 2,
		},
		{
			name:           "Set to true",
			startingPolicy: &policy.ErrorHandlingPolicy{},
			fileArg:        "true",
			dirArg:         "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			expChangeCount: 2,
		},
		{
			name: "Set to false",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			fileArg: "false",
			dirArg:  "false",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(false),
				IgnoreDirectoryErrors: policy.NewOptionalBool(false),
			},
			expChangeCount: 2,
		},
		{
			name: "File false, dir true",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(false),
			},
			fileArg: "false",
			dirArg:  "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(false),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			expChangeCount: 2,
		},
		{
			name: "File true, dir false",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(false),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			fileArg: "true",
			dirArg:  "false",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(false),
			},
			expChangeCount: 2,
		},
		{
			name: "File inherit, dir true",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: policy.NewOptionalBool(false),
			},
			fileArg: "inherit",
			dirArg:  "true",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      nil,
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			expChangeCount: 2,
		},
		{
			name: "File true, dir inherit",
			startingPolicy: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(false),
				IgnoreDirectoryErrors: policy.NewOptionalBool(true),
			},
			fileArg: "true",
			dirArg:  "inherit",
			expResult: &policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      policy.NewOptionalBool(true),
				IgnoreDirectoryErrors: nil,
			},
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

//nolint:maintidx
func TestSetSchedulingPolicyFromFlags(t *testing.T) {
	ctx := testlogging.Context(t)

	for _, tc := range []struct {
		name           string
		startingPolicy *policy.SchedulingPolicy
		intervalArg    []time.Duration
		timesOfDayArg  []string
		cronArg        string
		manualArg      bool
		runMissedArg   string
		expResult      *policy.SchedulingPolicy
		expErrMsg      string
		expChangeCount int
	}{
		{
			name:           "No flags provided, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			expResult:      &policy.SchedulingPolicy{},
			expChangeCount: 0,
		},
		{
			name:           "Manual flag set to true, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			manualArg:      true,
			expResult: &policy.SchedulingPolicy{
				Manual: true,
			},
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
			expErrMsg:      "cannot set manual field when scheduling snapshots",
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
			expErrMsg:      "cannot set manual field when scheduling snapshots",
			expChangeCount: 0,
		},
		{
			name:           "Manual and cron set, no starting policy",
			startingPolicy: &policy.SchedulingPolicy{},
			cronArg:        "* * * * *",
			manualArg:      true,
			expResult:      &policy.SchedulingPolicy{},
			expErrMsg:      "cannot set manual field when scheduling snapshots",
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
			expChangeCount: 1,
		},
		{
			name:           "Set single cron expression",
			startingPolicy: &policy.SchedulingPolicy{},
			cronArg:        "1 2 * * *",
			expResult: &policy.SchedulingPolicy{
				Cron: []string{"1 2 * * *"},
			},
			expChangeCount: 1,
		},
		{
			name:           "Set single cron expression with comment",
			startingPolicy: &policy.SchedulingPolicy{},
			cronArg:        "1 2 * * * # some comment",
			expResult: &policy.SchedulingPolicy{
				Cron: []string{"1 2 * * * # some comment"},
			},
			expChangeCount: 1,
		},
		{
			name:           "Support comment-only cron expression",
			startingPolicy: &policy.SchedulingPolicy{},
			cronArg:        "# some comment;1 2 * * * ",
			expResult: &policy.SchedulingPolicy{
				Cron: []string{"# some comment", "1 2 * * *"},
			},
			expChangeCount: 1,
		},
		{
			name:           "Set multiple cron expressions",
			startingPolicy: &policy.SchedulingPolicy{},
			cronArg:        ";1 2 * * *;;;2 1 * * *;",
			expResult: &policy.SchedulingPolicy{
				Cron: []string{"1 2 * * *", "2 1 * * *"},
			},
			expChangeCount: 1,
		},
		{
			name:           "Set invalid cron expression",
			startingPolicy: &policy.SchedulingPolicy{},
			cronArg:        "aa bb * * *",
			expErrMsg:      "invalid cron expression",
			expChangeCount: 1,
		},
		{
			name: "Inherit cron expressions",
			startingPolicy: &policy.SchedulingPolicy{
				Cron: []string{"1 2 * * *", "2 1 * * *"},
			},
			cronArg: "inherit",
			expResult: &policy.SchedulingPolicy{
				Cron: nil,
			},
			expChangeCount: 1,
		},
		{
			name: "Set RunMissed",
			startingPolicy: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
			},
			runMissedArg: "true",
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			expChangeCount: 1,
		},
		{
			name: "Clear RunMissed",
			startingPolicy: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
				RunMissed:  policy.NewOptionalBool(false),
			},
			runMissedArg:   "false",
			expChangeCount: 1,
		},
		{
			name: "RunMissed unchanged",
			startingPolicy: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			expResult: &policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{Hour: 12, Minute: 0}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			expChangeCount: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changeCount := 0

			var psf policySchedulingFlags

			psf.policySetInterval = tc.intervalArg
			psf.policySetTimesOfDay = tc.timesOfDayArg
			psf.policySetManual = tc.manualArg
			psf.policySetRunMissed = tc.runMissedArg
			psf.policySetCron = tc.cronArg

			err := psf.setSchedulingPolicyFromFlags(ctx, tc.startingPolicy, &changeCount)
			if tc.expErrMsg != "" {
				require.ErrorContains(t, err, tc.expErrMsg)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expResult, tc.startingPolicy)
			require.Equal(t, tc.expChangeCount, changeCount)
		})
	}
}
