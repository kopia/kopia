package policy_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/snapshot/policy"
)

//nolint:maintidx
func TestNextSnapshotTime(t *testing.T) {
	cases := []struct {
		name                 string
		pol                  policy.SchedulingPolicy
		now                  time.Time
		previousSnapshotTime time.Time
		wantTime             time.Time
		wantOK               bool
	}{
		{name: "empty policy, no snapshot"},
		{
			name:                 "next snapshot is 1 minute after last, which is in the past",
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 60},
			now:                  time.Date(2020, time.January, 1, 12, 3, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 12, 3, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name:                 "next snapshot is 1 min after last, which is in the future",
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 60},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 51, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name:                 "last snapshot was in the future, but next snapshot is 5 mins after that",
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 300},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 51, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name:                 "next time after 11:50 truncated to 20 full minutes, which is 12:00",
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 1200},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 12, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name:                 "next time after 11:50 truncated to 20 full minutes, which is 12:00",
			pol:                  policy.SchedulingPolicy{IntervalSeconds: 1200},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 12, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "multiple ToD schedules, next snapshot is the earliest",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}, {11, 57}},
			},
			now:                  time.Date(2020, time.January, 1, 11, 50, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "multiple ToD snapshots, next is the 2nd one",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}, {11, 57}},
			},
			now:      time.Date(2020, time.January, 1, 11, 55, 30, 0, time.Local),
			wantTime: time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantOK:   true,
		},
		{
			name: "interval and ToD policies, next is 1st ToD",
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
			name: "interval and ToD policies, next is now (1st ToD)",
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
			name: "interval and ToD policies, next is interval",
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
			name: "interval and ToD policies, next is now (interval)",
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
			name: "interval and ToD policies, next is now (interval overdue)",
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
			name: "multiple ToD policies, last missed, RunMissed is off, next is 2nd ToD",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 56, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "multiple ToD policies, last missed, RunMissed is off, next is now (2nd ToD)",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 1, 11, 57, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "multiple ToD policies, last missed, RunMissed is off, next is tomorrow",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 54}, {11, 57}},
			},
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 50, 0, 0, time.Local),
			now:                  time.Date(2020, time.January, 1, 11, 57, 0, 1, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 11, 54, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "interval and ToD policies, last 9hrs in the future, next is 1st ToD",
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
			name: "ToD policy and manual policies, manual wins",
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
		{
			name: "Cron policy using minute and hour rules",
			pol: policy.SchedulingPolicy{
				Cron:      []string{"0 23 * * *"},
				RunMissed: policy.NewOptionalBool(false),
			},
			now: time.Date(2020, time.January, 1, 10, 0, 0, 0, time.Local),
			// matches 23:00
			wantTime: time.Date(2020, time.January, 1, 23, 0, 0, 0, time.Local),
			wantOK:   true,
		},
		{
			name: "Cron policy using minute, hour, month, and day rules",
			pol: policy.SchedulingPolicy{
				Cron:      []string{"5 3 * Feb Thu"},
				RunMissed: policy.NewOptionalBool(false),
			},
			now: time.Date(2020, time.January, 1, 1, 0, 0, 0, time.Local),
			// matches next Thursday in February, 3:05
			wantTime: time.Date(2020, time.February, 6, 3, 5, 0, 0, time.Local),
			wantOK:   true,
		},
		{
			name: "Run immediately since last run was missed and RunMissed is set",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			now:                  time.Date(2020, time.January, 2, 11, 55, 30, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 11, 55, 30, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "Don't run immediately even though RunMissed is set, because next run is upcoming",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			now:                  time.Date(2020, time.January, 3, 11, 30, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 3, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "Run immediately because one of the TimeOfDays was missed",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 1}, {4, 1}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			now:                  time.Date(2020, time.January, 2, 10, 0, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 1, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 10, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "Don't run immediately even though RunMissed is set because last run was not missed",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			now:                  time.Date(2020, time.January, 2, 11, 30, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 11, 55, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "Don't run immediately even though RunMissed is set because last run was not missed",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{10, 0}},
				RunMissed:  policy.NewOptionalBool(true),
			},
			now:                  time.Date(2020, time.January, 2, 11, 0, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 11, 0, 0, 0, time.Local),
			wantOK:               true,
		},
		{
			name: "Run immediately because Cron was missed",
			pol: policy.SchedulingPolicy{
				TimesOfDay: []policy.TimeOfDay{{11, 55}},
				Cron:       []string{"0 * * * *"}, // Every hour
				RunMissed:  policy.NewOptionalBool(true),
			},
			now:                  time.Date(2020, time.January, 2, 11, 0, 0, 0, time.Local),
			previousSnapshotTime: time.Date(2020, time.January, 1, 11, 55, 0, 0, time.Local),
			wantTime:             time.Date(2020, time.January, 2, 11, 0, 0, 0, time.Local),
			wantOK:               true,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			gotTime, gotOK := tc.pol.NextSnapshotTime(tc.previousSnapshotTime, tc.now)
			require.Equal(t, tc.wantTime, gotTime, tc.name)
			require.Equal(t, tc.wantOK, gotOK, tc.name)
		})
	}
}

func TestSortAndDedupeTimesOfDay(t *testing.T) {
	cases := []struct {
		input []policy.TimeOfDay
		want  []policy.TimeOfDay
	}{
		{},
		{
			input: []policy.TimeOfDay{{Hour: 10, Minute: 23}},
			want:  []policy.TimeOfDay{{Hour: 10, Minute: 23}},
		},
		{
			input: []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}},
			want:  []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}},
		},
		{
			input: []policy.TimeOfDay{{Hour: 11, Minute: 25}, {Hour: 10, Minute: 23}},
			want:  []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}},
		},
		{
			input: []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 10, Minute: 23}},
			want:  []policy.TimeOfDay{{Hour: 10, Minute: 23}},
		},
		{
			input: []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}},
			want:  []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}},
		},
		{
			input: []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}, {Hour: 11, Minute: 25}},
			want:  []policy.TimeOfDay{{Hour: 10, Minute: 23}, {Hour: 11, Minute: 25}},
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			got := policy.SortAndDedupeTimesOfDay(tc.input)
			require.Equal(t, tc.want, got)
		})
	}
}
