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

func TestBuildExtra(t *testing.T) {
	um := unmarshalable{
		Data: "fake",
	}

	cases := []struct {
		name        string
		stats       Kind
		expected    Extra
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
		{
			name: "succeed",
			stats: &CleanupMarkersStats{
				DeletedEpochMarkerBlobs:       10,
				DeletedDeletionWaterMarkBlobs: 20,
			},
			expected: Extra{
				Kind: "cleanupMarkersStats",
				Data: []byte("{\"deletedEpochMarkerBlobs\":10,\"deletedDeletionWaterMarkBlobs\":20}"),
			},
		},
	}

	for _, tc := range cases {
		result, err := BuildExtra(tc.stats)
		if tc.expectedErr == "" {
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		} else {
			require.EqualError(t, err, tc.expectedErr)
		}
	}
}

func TestBuildFromExtra(t *testing.T) {
	cases := []struct {
		name        string
		stats       Extra
		expected    Summarizer
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
			expectedErr: "error unmarshaling raw stats []: unexpected end of JSON input",
		},
		{
			name: "cleanupMarkersStats",
			stats: Extra{
				Kind: cleanupMarkersStatsKind,
				Data: []byte("{\"deletedEpochMarkerBlobs\":10,\"deletedDeletionWaterMarkBlobs\":20}"),
			},
			expected: &CleanupMarkersStats{
				DeletedEpochMarkerBlobs:       10,
				DeletedDeletionWaterMarkBlobs: 20,
			},
		},
	}

	for _, tc := range cases {
		result, err := BuildFromExtra(tc.stats)
		if tc.expectedErr == "" {
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		} else {
			require.EqualError(t, err, tc.expectedErr)
		}
	}
}
