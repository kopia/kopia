package maintenance

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	t0700 = time.Date(2020, 1, 1, 7, 0, 0, 0, time.UTC)
	t0715 = time.Date(2020, 1, 1, 7, 15, 0, 0, time.UTC)
	t0900 = time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)
	t0915 = time.Date(2020, 1, 1, 9, 15, 0, 0, time.UTC)
	t1300 = time.Date(2020, 1, 1, 13, 0, 0, 0, time.UTC)
	t1315 = time.Date(2020, 1, 1, 13, 15, 0, 0, time.UTC)
)

func TestShouldDeleteOrphanedBlobs(t *testing.T) {
	now := t1315

	cases := []struct {
		runs   map[TaskType][]RunInfo
		safety SafetyParameters
		want   bool
	}{
		{
			// no rewrites
			runs:   map[TaskType][]RunInfo{},
			safety: SafetyFull,
			want:   true,
		},
		{
			runs:   map[TaskType][]RunInfo{},
			safety: SafetyNone,
			want:   true,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskRewriteContentsQuick: {
					// old enough
					{End: t0900, Success: true},
				},
			},
			safety: SafetyFull,
			want:   true,
		},
		{
			runs: map[TaskType][]RunInfo{
				// recent but no safety, so will go through
				TaskRewriteContentsFull: {
					{End: t1300, Success: true},
				},
			},
			safety: SafetyNone,
			want:   true,
		},
		{
			runs: map[TaskType][]RunInfo{
				// too recent for full safety
				TaskRewriteContentsFull: {
					{End: t1300, Success: true},
				},
			},
			safety: SafetyFull,
			want:   false,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			require.Equal(t, tc.want, shouldDeleteOrphanedPacks(now, &Schedule{
				Runs: tc.runs,
			}, tc.safety))
		})
	}
}

func TestShouldRewriteContents(t *testing.T) {
	cases := []struct {
		runs      map[TaskType][]RunInfo
		safety    SafetyParameters
		wantFull  bool
		wantQuick bool
	}{
		{
			runs:      map[TaskType][]RunInfo{},
			wantFull:  true,
			wantQuick: true,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskDeleteOrphanedBlobsFull: {
					RunInfo{Success: true, End: t0715},
				},
				TaskDeleteOrphanedBlobsQuick: {
					RunInfo{Success: true, End: t0700},
				},
			},
			safety:    SafetyFull,
			wantFull:  true,
			wantQuick: true,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskDeleteOrphanedBlobsQuick: {
					RunInfo{Success: true, End: t0700},
				},
				TaskRewriteContentsFull: {
					RunInfo{Success: true, End: t0715},
				},
			},
			safety:    SafetyFull,
			wantFull:  false,
			wantQuick: false,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskDeleteOrphanedBlobsQuick: {
					RunInfo{Success: true, End: t0700},
				},
				TaskRewriteContentsFull: {
					RunInfo{Success: true, End: t0715},
				},
			},
			safety:    SafetyNone,
			wantFull:  true,
			wantQuick: true,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskDeleteOrphanedBlobsQuick: {
					RunInfo{Success: true, End: t0700},
				},
				TaskRewriteContentsQuick: {
					RunInfo{Success: true, End: t0715},
				},
			},
			safety:    SafetyFull,
			wantFull:  true, // will be allowed despite quick run having just finished
			wantQuick: false,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskDeleteOrphanedBlobsQuick: {
					RunInfo{Success: true, End: t0700},
				},
				TaskRewriteContentsQuick: {
					RunInfo{Success: true, End: t0715},
				},
			},
			safety:    SafetyNone,
			wantFull:  true, // will be allowed despite quick run having just finished
			wantQuick: true,
		},
		{
			runs: map[TaskType][]RunInfo{
				TaskDeleteOrphanedBlobsQuick: {
					RunInfo{Success: true, End: t0715},
				},
				TaskRewriteContentsFull: {
					RunInfo{Success: true, End: t0700},
				},
			},
			safety:    SafetyFull,
			wantFull:  true,
			wantQuick: true,
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.wantQuick, shouldQuickRewriteContents(&Schedule{
			Runs: tc.runs,
		}, tc.safety), tc.runs)
		require.Equal(t, tc.wantFull, shouldFullRewriteContents(&Schedule{
			Runs: tc.runs,
		}, tc.safety), tc.runs)
	}
}

func TestFindSafeDropTime(t *testing.T) {
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
			wantTime: t0700.Add(-SafetyFull.DropContentFromIndexExtraMargin),
		},
		// three runs spaced enough
		{
			runs: []RunInfo{
				{Start: t0700, End: t0715, Success: true},
				{Start: t0900, End: t0915, Success: true},
				{Start: t1300, End: t1315, Success: true},
			},
			wantTime: t0700.Add(-SafetyFull.DropContentFromIndexExtraMargin),
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
		got := findSafeDropTime(tc.runs, SafetyFull)
		require.Equalf(t, tc.wantTime, got, "invalid safe drop time for %v", tc.runs)
	}
}
