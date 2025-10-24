package maintenancestats

import (
	"testing"

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
			name: "succeed",
			stats: &CleanupMarkersStats{
				DeletedEpochMarkerBlobCount:       10,
				DeletedDeletionWaterMarkBlobCount: 20,
			},
			expected: Extra{
				Kind: "cleanupMarkersStats",
				Data: []byte(`{"deletedEpochMarkerBlobCount":10,"deletedDeletionWaterMarkBlobCount":20}`),
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
				Data: []byte(`{"deletedEpochMarkerBlobCount":10,"deletedDeletionWaterMarkBlobCount":20}`),
			},
			expected: &CleanupMarkersStats{
				DeletedEpochMarkerBlobCount:       10,
				DeletedDeletionWaterMarkBlobCount: 20,
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
