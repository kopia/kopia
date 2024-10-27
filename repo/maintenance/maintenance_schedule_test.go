package maintenance_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/maintenance"
)

func (s *formatSpecificTestSuite) TestMaintenanceSchedule(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion)
	sch, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)

	require.NoError(t, err)
	require.True(t, sch.NextFullMaintenanceTime.IsZero(), "unexpected NextFullMaintenanceTime")
	require.True(t, sch.NextQuickMaintenanceTime.IsZero(), "unexpected NextQuickMaintenanceTime")

	sch.NextFullMaintenanceTime = clock.Now()
	sch.NextQuickMaintenanceTime = clock.Now()
	sch.ReportRun("foo", maintenance.RunInfo{
		Start:   clock.Now(),
		End:     clock.Now(),
		Success: true,
	})

	err = maintenance.SetSchedule(ctx, env.RepositoryWriter, sch)
	require.NoError(t, err, "unable to set schedule")

	s2, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)
	require.NoError(t, err, "unable to get schedule")

	got, want := toJSON(t, s2), toJSON(t, sch)
	require.Equal(t, want, got, "unexpected schedule")
}

func TestTimeToAttemptNextMaintenance(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	now := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		desc   string
		params maintenance.Params
		sched  maintenance.Schedule
		want   time.Time
	}{
		{
			desc: "both enabled, quick first",
			params: maintenance.Params{
				Owner:      env.Repository.ClientOptions().UsernameAtHost(),
				QuickCycle: maintenance.CycleParams{Enabled: true},
				FullCycle:  maintenance.CycleParams{Enabled: true},
			},
			sched: maintenance.Schedule{
				NextFullMaintenanceTime:  now.Add(3 * time.Hour),
				NextQuickMaintenanceTime: now.Add(1 * time.Hour),
			},
			want: now.Add(1 * time.Hour),
		},
		{
			desc: "both enabled, full first",
			params: maintenance.Params{
				Owner:      env.Repository.ClientOptions().UsernameAtHost(),
				QuickCycle: maintenance.CycleParams{Enabled: true},
				FullCycle:  maintenance.CycleParams{Enabled: true},
			},
			sched: maintenance.Schedule{
				NextFullMaintenanceTime:  now.Add(2 * time.Hour),
				NextQuickMaintenanceTime: now.Add(3 * time.Hour),
			},
			want: now.Add(2 * time.Hour),
		},
		{
			desc: "both disabled",
			params: maintenance.Params{
				Owner:      env.Repository.ClientOptions().UsernameAtHost(),
				QuickCycle: maintenance.CycleParams{Enabled: false},
				FullCycle:  maintenance.CycleParams{Enabled: false},
			},
			sched: maintenance.Schedule{
				NextFullMaintenanceTime:  now.Add(2 * time.Hour),
				NextQuickMaintenanceTime: now.Add(3 * time.Hour),
			},
			want: time.Time{},
		},
		{
			desc: "not owned",
			params: maintenance.Params{
				Owner:      "some-other-owner",
				QuickCycle: maintenance.CycleParams{Enabled: true},
				FullCycle:  maintenance.CycleParams{Enabled: true},
			},
			sched: maintenance.Schedule{
				NextFullMaintenanceTime:  now.Add(2 * time.Hour),
				NextQuickMaintenanceTime: now.Add(3 * time.Hour),
			},
			want: time.Time{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, maintenance.SetParams(ctx, env.RepositoryWriter, &tc.params))
			require.NoError(t, maintenance.SetSchedule(ctx, env.RepositoryWriter, &tc.sched))

			nmt, err := maintenance.TimeToAttemptNextMaintenance(ctx, env.RepositoryWriter)
			require.NoError(t, err)

			require.Equal(t, tc.want, nmt)
		})
	}
}

func toJSON(t *testing.T, v interface{}) string {
	t.Helper()

	b, err := json.MarshalIndent(v, "", "  ")

	require.NoError(t, err, "json marshal")

	return string(b)
}
