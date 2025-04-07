package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
)

func TestRestoreSnapshotMaxTime(t *testing.T) {
	t.Parallel()

	now := clock.Now()
	ago := func(y, m, d int) time.Time {
		r := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return r.AddDate(y, m, d)
	}
	at := func(y, mo, d, h, m, s int) time.Time {
		return time.Date(y, time.Month(mo), d, h, m, s, 0, now.Location())
	}

	requireTime := func(expected time.Time, timespect string) {
		mt, err := computeMaxTime(timespect)
		require.NoError(t, err)
		require.Equal(t, expected, mt)
	}

	requireTime(ago(0, 0, 0), "yesterday")
	requireTime(ago(0, 0, 0), "1d-ago")
	requireTime(ago(0, 0, 0), "1day-ago")

	requireTime(at(now.Year(), int(now.Month()), 1, 0, 0, 0), "last-month")
	requireTime(at(now.Year(), 1, 1, 0, 0, 0), "last-year")

	requireTime(ago(0, -1, 1), "1month-ago")
	requireTime(ago(0, -1, 1), "1m-ago")
	requireTime(ago(-1, 0, 1), "1year-ago")
	requireTime(ago(-1, 0, 1), "1y-ago")
	requireTime(ago(-2, -2, -1), "2years-2months-2days-ago")
	requireTime(ago(-2, -2, -1), "2y-2m-2d-ago")

	requireTime(at(2020, 1, 1, 0, 0, 0), "2019")
	requireTime(at(2019, 2, 1, 0, 0, 0), "2019-1")
	requireTime(at(2019, 1, 2, 0, 0, 0), "2019-01-1")
	requireTime(at(2019, 1, 1, 14, 0, 0), "2019-01-1 13")
	requireTime(at(2019, 1, 1, 13, 2, 0), "2019-01-1 13:01")
	requireTime(at(2019, 1, 1, 13, 1, 16), "2019-01-1 13:01:15")
}

func TestRestoreSnapshotFilter(t *testing.T) {
	f, err := createSnapshotTimeFilter("latest")
	require.NoError(t, err)
	require.True(t, f(nil, 0, 2))
	require.False(t, f(nil, 1, 2))

	f, err = createSnapshotTimeFilter("oldest")
	require.NoError(t, err)
	require.False(t, f(nil, 0, 2))
	require.True(t, f(nil, 1, 2))
}
