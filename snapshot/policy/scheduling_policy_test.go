package policy_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/snapshot/policy"
)

func TestNextSnapshotTime(t *testing.T) {
	cases := []struct {
		pol                  policy.SchedulingPolicy
		now                  time.Time
		previousSnapshotTime time.Time
		wantTime             time.Time
		wantOK               bool
	}{
		{}, // empty policy, no snapshot
		{
			// next snapshot is 1 minute after last, which is in the past
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 60},
			now:                  time.Date(2020, time.January, 1, 12, 3, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 12, 3, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 60},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 51, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 300},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 51, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			// next time after 11:50 truncated to 20 full minutes, which is 12:00
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 1200},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 12, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			// next time after 11:50 truncated to 20 full minutes, which is 12:00
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 1200},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 12, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}, {11, 57}},
			},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}, {11, 57}},
			},
			now:      time.Date(2020, time.January, 1, 11, 55, 30, 0, time.Local),
			wantTime: time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantOK:   true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 300, // every 5 minutes
				TimesOfDay:      []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 53, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 54, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 300, // every 5 minutes
				TimesOfDay:      []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 54, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 54, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 300, // every 5 minutes
				TimesOfDay:      []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 54, 1, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 300, // every 5 minutes
				TimesOfDay:      []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 300, // every 5 minutes
				TimesOfDay:      []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 55, 1, 0, time.Local),
			// interval-based snapshot is overdue
			wantTime: time.Date(2020, time.January, 1, 11, 55, 1, 0, time.Local),
			wantOK:   true,
		},
		{
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 56, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 57, 0, 1, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 11, 54, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 43200,
				TimesOfDay:      []policy.TimeOfDay{{19, 0}, {20, 0}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 19, 0, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 10, 0, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 19, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			pol: policy.SchedulingPolicy{
				IntervalSeconds: 43200,
				TimesOfDay:      []policy.TimeOfDay{{19, 0}, {20, 0}},
				Manual:          true,
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 19, 0, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 10, 0, 0, 0, time.Local),
			wantTime:             time.Time{},
			wantOK:               false,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			gotTime, gotOK := tc.pol.NextSnapshotTime(tc.previousSnapshotTime, tc.now)

			require.Equal(t, tc.wantTime, gotTime)
			require.Equal(t, tc.wantOK, gotOK)
		})
	}
}
