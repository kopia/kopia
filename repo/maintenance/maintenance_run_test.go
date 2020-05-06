package maintenance

import (
	"testing"
	"time"
)

func TestFindSafeDropTime(t *testing.T) {
	var (
		t0700 = time.Date(2020, 1, 1, 7, 0, 0, 0, time.UTC)
		t0715 = time.Date(2020, 1, 1, 7, 15, 0, 0, time.UTC)
		t0900 = time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)
		t0915 = time.Date(2020, 1, 1, 9, 15, 0, 0, time.UTC)
		t1300 = time.Date(2020, 1, 1, 13, 0, 0, 0, time.UTC)
		t1315 = time.Date(2020, 1, 1, 13, 15, 0, 0, time.UTC)
	)

	cases := []struct {
		runs     []RunInfo
		wantTime time.Time
	}{
		// no runs
		{
			runs:     nil,
			wantTime: time.Time{},
		},
		// one run, not enough
		{
			runs: []RunInfo{
				{Start: t1300, End: t1315, Success: true},
			},
			wantTime: time.Time{},
		},
		// two runs not spaced enough
		{
			runs: []RunInfo{
				{Start: t0700, End: t0715, Success: true},
				{Start: t0900, End: t0915, Success: true},
			},
			wantTime: time.Time{},
		},
		// two runs spaced enough
		{
			runs: []RunInfo{
				{Start: t0700, End: t0715, Success: true},
				{Start: t1300, End: t1315, Success: true},
			},
			wantTime: t0700.Add(extraSafetyMarginBeforeDroppingContentFromIndex),
		},
		// three runs spaced enough
		{
			runs: []RunInfo{
				{Start: t0700, End: t0715, Success: true},
				{Start: t0900, End: t0915, Success: true},
				{Start: t1300, End: t1315, Success: true},
			},
			wantTime: t0700.Add(extraSafetyMarginBeforeDroppingContentFromIndex),
		},
		// three runs spaced enough, not successful
		{
			runs: []RunInfo{
				{Start: t0700, End: t0715, Success: false},
				{Start: t0900, End: t0915, Success: true},
				{Start: t1300, End: t1315, Success: true},
			},
			wantTime: time.Time{},
		},
	}

	for _, tc := range cases {
		if got, want := findSafeDropTime(tc.runs), tc.wantTime; !got.Equal(want) {
			t.Errorf("invalid safe drop time for %v: %v, want %v", tc.runs, got, want)
		}
	}
}
