package metrics_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestTimeResolutions(t *testing.T) {
	cases := []struct {
		description string
		t           time.Time
		resolution  metrics.TimeResolutionFunc
		wantStart   time.Time
		wantEnd     time.Time
	}{
		{
			"day resolution",
			dayOf(2021, 1, 1),
			metrics.TimeResolutionByDay,
			dayOf(2021, 1, 1),
			dayOf(2021, 1, 2),
		},
		{
			"week (Sunday-based) resolution",
			dayOf(2021, 1, 1),
			metrics.TimeResolutionByWeekStartingSunday,
			dayOf(2020, 12, 27),
			dayOf(2021, 1, 3),
		},
		{
			"week (Monday-based) resolution",
			dayOf(2021, 1, 1),
			metrics.TimeResolutionByWeekStartingMonday,
			dayOf(2020, 12, 28),
			dayOf(2021, 1, 4),
		},
		{
			"quarterly resolution",
			dayOf(2021, 5, 1),
			metrics.TimeResolutionByQuarter,
			monthOf(2021, 4),
			monthOf(2021, 7),
		},
		{
			"yearly resolution",
			dayOf(2021, 5, 1),
			metrics.TimeResolutionByYear,
			monthOf(2021, 1),
			monthOf(2022, 1),
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			start, end := tc.resolution(tc.t)
			require.Equal(t, tc.wantStart, start)
			require.Equal(t, tc.wantEnd, end)

			start1, end1 := tc.resolution(tc.wantStart)
			require.Equal(t, tc.wantStart, start1)
			require.Equal(t, tc.wantEnd, end1)

			// last possible moment still maps to the same time period
			start2, end2 := tc.resolution(tc.wantEnd.Add(-1))
			require.Equal(t, tc.wantStart, start2)
			require.Equal(t, tc.wantEnd, end2)

			midPoint := tc.wantStart.Add(tc.wantEnd.Sub(tc.wantStart) / 2)

			start3, end3 := tc.resolution(midPoint)
			require.Equal(t, tc.wantStart, start3)
			require.Equal(t, tc.wantEnd, end3)
		})
	}
}
