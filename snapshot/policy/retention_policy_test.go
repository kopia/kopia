package policy

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

func TestRetentionPolicyTest(t *testing.T) {
	cases := []struct {
		retentionPolicy    *RetentionPolicy
		timeToExpectedTags map[string][]string
	}{
		{
			&RetentionPolicy{}, // empty policy is treated as {"keepLatest": maxInt}
			map[string][]string{
				"2020-01-01T12:00:00Z": {"latest-4"},
				"2020-01-01T12:01:00Z": {"latest-3"},
				"2020-01-01T12:02:00Z": {"latest-2"},
				"2020-01-01T12:03:00Z": {"latest-1"},
			},
		},
		{
			&RetentionPolicy{
				KeepLatest: newOptionalInt(3),
			},
			map[string][]string{
				"2020-01-01T12:00:00Z": {}, // not retained, only keep 3 latest
				"2020-01-01T12:01:00Z": {"latest-3"},
				"2020-01-01T12:02:00Z": {"latest-2"},
				"2020-01-01T12:03:00Z": {"latest-1"},
			},
		},
		{
			&RetentionPolicy{
				KeepDaily: newOptionalInt(3),
			},
			map[string][]string{
				"2020-01-01T12:00:00Z": {},
				"2020-01-01T15:00:00Z": {}, // not retained since it's before now - 3 days
				"2020-01-02T12:00:00Z": {}, // not retained since there's a newer snapshot for that day
				"2020-01-02T15:00:00Z": {"daily-3"},
				"2020-01-03T12:00:00Z": {}, // not retained since there's a newer snapshot for that day
				"2020-01-03T15:00:00Z": {"daily-2"},
				"2020-01-04T12:00:00Z": {}, // not retained since there's a newer snapshot for that day
				"2020-01-04T15:00:00Z": {"daily-1"},
			},
		},
		{
			&RetentionPolicy{
				KeepMonthly: newOptionalInt(3),
			},
			map[string][]string{
				"2020-01-01T12:00:00Z": {},
				"2020-01-01T15:00:00Z": {},
				"2020-02-01T12:00:00Z": {},
				"2020-02-02T15:00:00Z": {"monthly-3"},
				"2020-03-01T12:00:00Z": {},
				"2020-03-02T15:00:00Z": {"monthly-2"},
				"2020-04-01T12:00:00Z": {},
				"2020-04-02T15:00:00Z": {"monthly-1"},
			},
		},
		{
			&RetentionPolicy{
				KeepMonthly: newOptionalInt(3),
			},
			map[string][]string{
				"2020-01-01T12:00:00Z": {},
				"2020-01-01T15:00:00Z": {},
				// no snapshots in february, but since the latest snapshot is in April, January is also not included
				"2020-03-02T15:00:00Z": {"monthly-2"},
				"2020-04-01T12:00:00Z": {},
				"2020-04-02T15:00:00Z": {"monthly-1"},
			},
		},
		{
			&RetentionPolicy{
				KeepLatest:  newOptionalInt(3),
				KeepHourly:  newOptionalInt(7),
				KeepDaily:   newOptionalInt(5),
				KeepMonthly: newOptionalInt(2),
			},
			map[string][]string{
				"2020-01-01T12:00:00Z": {},
				"2020-01-01T13:00:00Z": {},
				"2020-01-01T14:00:00Z": {},
				"2020-01-01T15:00:00Z": {},
				"2020-01-02T12:00:00Z": {},
				"2020-01-02T13:00:00Z": {},
				"2020-01-02T15:00:00Z": {},

				"2020-02-01T12:00:00Z": {},
				"2020-02-01T13:00:00Z": {},
				"2020-02-01T14:00:00Z": {},
				"2020-02-01T15:00:00Z": {},
				"2020-02-02T12:00:00Z": {},
				"2020-02-02T13:00:00Z": {},
				"2020-02-02T15:00:00Z": {},

				"2020-03-01T12:00:00Z": {},
				"2020-03-01T13:00:00Z": {},
				"2020-03-01T14:00:00Z": {},
				"2020-03-01T15:00:00Z": {},
				"2020-03-02T12:00:00Z": {},
				"2020-03-02T13:00:00Z": {},
				"2020-03-02T15:00:00Z": {"monthly-2"},

				"2020-04-01T12:00:00Z":            {},
				"2020-04-01T13:00:00Z":            {},
				"2020-04-01T14:00:00Z":            {},
				"2020-04-01T15:00:00Z":            {"daily-2"},
				"2020-04-02T12:00:00Z":            {"latest-3", "hourly-3"},
				"2020-04-02T13:00:00Z":            {"latest-2", "hourly-2"},
				"2020-04-02T15:00:00Z":            {"latest-1", "hourly-1", "daily-1", "monthly-1"},
				"incomplete-2020-04-02T15:01:00Z": {}, // incomplete, too old
				"incomplete-2020-04-02T16:01:00Z": {}, // incomplete, too old
				"incomplete-2020-04-02T17:01:00Z": {}, // incomplete, too old
				"incomplete-2020-04-02T18:01:00Z": {}, // incomplete, too old
				"incomplete-2020-04-02T19:01:00Z": {}, // incomplete, too old
				"incomplete-2020-04-02T20:01:00Z": {"incomplete"},
				"incomplete-2020-04-02T21:01:00Z": {"incomplete"},
				"incomplete-2020-04-02T22:01:00Z": {"incomplete"},
				"incomplete-2020-04-02T23:01:00Z": {"incomplete"},
				"incomplete-2020-04-02T23:50:00Z": {"incomplete"},
			},
		},
		{
			&RetentionPolicy{
				KeepLatest: newOptionalInt(3),
			},
			map[string][]string{
				"2020-04-02T15:00:00Z":            {"latest-1"},
				"incomplete-2020-04-02T15:01:00Z": {},             // incomplete, too old
				"incomplete-2020-04-02T16:01:00Z": {},             // incomplete, too old
				"incomplete-2020-04-02T17:01:00Z": {},             // incomplete, too old
				"incomplete-2020-04-02T18:01:00Z": {"incomplete"}, // incomplete, too old but still included because of count
				"incomplete-2020-04-02T19:01:00Z": {"incomplete"}, // incomplete, too old but still included because of count
				"incomplete-2020-04-02T23:50:00Z": {"incomplete"},
			},
		},
		{
			&RetentionPolicy{
				KeepWeekly: newOptionalInt(3),
			},
			map[string][]string{
				"2020-01-01T12:00:00Z": {},
				"2020-01-01T15:00:00Z": {"weekly-3"},
				"2020-01-08T12:00:00Z": {},
				"2020-01-09T12:00:00Z": {"weekly-2"},
				"2020-01-15T12:00:00Z": {"weekly-1"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			var manifests []*snapshot.Manifest
			var manifests2 []*snapshot.Manifest

			for ts, want := range tc.timeToExpectedTags {
				incompleteReason := ""
				if strings.HasPrefix(ts, "incomplete-") {
					incompleteReason = "some-reason"
				}
				startTime, err := time.Parse(time.RFC3339, strings.TrimPrefix(ts, "incomplete-"))
				if err != nil {
					t.Fatal(err)
				}

				manifests = append(manifests, &snapshot.Manifest{
					// store original ts to get it back quicker
					Description:      ts,
					StartTime:        fs.UTCTimestampFromTime(startTime),
					IncompleteReason: incompleteReason,
				})

				if len(want) != 0 {
					manifests2 = append(manifests2, &snapshot.Manifest{
						// store original ts to get it back quicker
						Description:      ts,
						StartTime:        fs.UTCTimestampFromTime(startTime),
						IncompleteReason: incompleteReason,
					})
				}
			}

			tc.retentionPolicy.ComputeRetentionReasons(manifests)
			tc.retentionPolicy.ComputeRetentionReasons(manifests2)

			for _, m := range manifests {
				gotRetentionReasons := m.RetentionReasons
				wantRetentionReasons := tc.timeToExpectedTags[m.Description]

				if diff := cmp.Diff(gotRetentionReasons, wantRetentionReasons); diff != "" {
					t.Errorf("unexpected retention reasons for snapshot at %v diff: %v", m.Description, diff)
				}
			}

			for _, m := range manifests2 {
				gotRetentionReasons := m.RetentionReasons
				wantRetentionReasons := tc.timeToExpectedTags[m.Description]

				if diff := cmp.Diff(gotRetentionReasons, wantRetentionReasons); diff != "" {
					t.Errorf("unexpected retention reasons for snapshot at %v diff: %v", m.Description, diff)
				}
			}
		})
	}
}

func TestCompactPins(t *testing.T) {
	require.Equal(t,
		[]string{"a", "b", "d", "x", "z"},
		CompactPins([]string{
			"z", "x", "a", "b", "d", "b", "z",
		}))
}

func TestCompactRetentionrRasons(t *testing.T) {
	cases := []struct {
		input []string
		want  []string
	}{
		{input: nil, want: []string{}},
		{[]string{"latest-1", "latest-2"}, []string{"latest-1..2"}},
		{[]string{"latest-1", "daily-3", "latest-2", "daily-2"}, []string{"latest-1..2", "daily-2..3"}},
		{[]string{"latest-1", "weekly-7", "latest-2"}, []string{"latest-1..2", "weekly-7"}},
		{[]string{"latest-1", "latest-2", "latest-5", "latest-6", "latest-7"}, []string{"latest-1..2", "latest-5..7"}},
		{[]string{"latest-1", "zrogue", "arogue", "latest-2"}, []string{"arogue", "zrogue", "latest-1..2"}},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, CompactRetentionReasons(tc.input))
	}
}
