package maintenancestats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type unmarshalable struct {
	Data    string
	Channel chan int
}

func (u *unmarshalable) Kind() string {
	return u.Data
}

func TestBuildExtraSuccess(t *testing.T) {
	cases := []struct {
		name     string
		stats    Kind
		expected Extra
	}{
		{
			name: "CleanupMarkersStats",
			stats: &CleanupMarkersStats{
				DeletedEpochMarkerBlobCount: 10,
				DeletedWatermarkBlobCount:   20,
			},
			expected: Extra{
				Kind: "cleanupMarkersStats",
				Data: []byte(`{"deletedEpochMarkerBlobCount":10,"deletedWatermarkBlobCount":20}`),
			},
		},
		{
			name: "GenerateRangeCheckpointStats",
			stats: &GenerateRangeCheckpointStats{
				RangeMinEpoch: 3,
				RangeMaxEpoch: 5,
			},
			expected: Extra{
				Kind: generateRangeCheckpointStatsKind,
				Data: []byte(`{"rangeMinEpoch":3,"rangeMaxEpoch":5}`),
			},
		},
	}

	for _, tc := range cases {
		result, err := BuildExtra(tc.stats)

		require.NoError(t, err)
		require.Equal(t, tc.expected, result)
	}
}

func TestBuildExtraError(t *testing.T) {
	um := unmarshalable{
		Data: "fake",
	}

	cases := []struct {
		name        string
		stats       Kind
		expectedErr string
	}{
		{
			name:        "nil stats",
			expectedErr: "invalid stats",
		},
		{
			name:        "marshal fails",
			stats:       &um,
			expectedErr: "error marshaling stats &{fake <nil>}: json: unsupported type: chan int",
		},
	}

	for _, tc := range cases {
		result, err := BuildExtra(tc.stats)

		require.EqualError(t, err, tc.expectedErr)
		require.Equal(t, Extra{}, result)
	}
}

func TestBuildFromExtraSuccess(t *testing.T) {
	cases := []struct {
		name     string
		stats    Extra
		expected Summarizer
	}{
		{
			name: "cleanupMarkersStats",
			stats: Extra{
				Kind: cleanupMarkersStatsKind,
				Data: []byte(`{"deletedEpochMarkerBlobCount":10,"deletedWatermarkBlobCount":20}`),
			},
			expected: &CleanupMarkersStats{
				DeletedEpochMarkerBlobCount: 10,
				DeletedWatermarkBlobCount:   20,
			},
		},
		{
			name: "cleanupSupersededIndexesStats",
			stats: Extra{
				Kind: cleanupSupersededIndexesStatsKind,
				Data: []byte(`{"maxReplacementTime":"2025-01-01T00:00:00Z","deletedBlobCount":10,"deletedTotalSize":1024}`),
			},
			expected: &CleanupSupersededIndexesStats{
				MaxReplacementTime: time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC),
				DeletedBlobCount:   10,
				DeletedTotalSize:   1024,
			},
		},
		{
			name: "generateRangeCheckpointStats",
			stats: Extra{
				Kind: generateRangeCheckpointStatsKind,
				Data: []byte(`{"rangeMinEpoch":3,"rangeMaxEpoch":5}`),
			},
			expected: &GenerateRangeCheckpointStats{
				RangeMinEpoch: 3,
				RangeMaxEpoch: 5,
			},
		},
	}

	for _, tc := range cases {
		result, err := BuildFromExtra(tc.stats)

		require.NoError(t, err)
		require.Equal(t, tc.expected, result)
	}
}

func TestBuildFromExtraError(t *testing.T) {
	cases := []struct {
		name        string
		stats       Extra
		expectedErr string
	}{
		{
			name:        "unsupported kind",
			expectedErr: "invalid kind for stats { []}: unsupported stats kind",
		},
		{
			name: "unmarshal fails",
			stats: Extra{
				Kind: cleanupMarkersStatsKind,
			},
			expectedErr: "error unmarshaling raw stats [] of kind cleanupMarkersStats to *maintenancestats.CleanupMarkersStats: unexpected end of JSON input",
		},
	}

	for _, tc := range cases {
		result, err := BuildFromExtra(tc.stats)

		require.EqualError(t, err, tc.expectedErr)
		require.Nil(t, result)
	}
}
