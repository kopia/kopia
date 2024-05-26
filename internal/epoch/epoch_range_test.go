package epoch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLongestRangeCheckpoint(t *testing.T) {
	m0_9 := newEpochRangeMetadataForTesting(0, 9)
	m0_29 := newEpochRangeMetadataForTesting(0, 29)
	m10_19 := newEpochRangeMetadataForTesting(10, 19)
	m20_29 := newEpochRangeMetadataForTesting(20, 29)
	m30_39 := newEpochRangeMetadataForTesting(30, 39)
	m40_49 := newEpochRangeMetadataForTesting(40, 49)
	m50_59 := newEpochRangeMetadataForTesting(50, 59)
	m10_59 := newEpochRangeMetadataForTesting(10, 59)

	cases := []struct {
		input []*RangeMetadata
		want  []*RangeMetadata
	}{
		{
			input: nil,
			want:  nil,
		},
		{
			input: []*RangeMetadata{m0_9, m10_19, m20_29},
			want:  []*RangeMetadata{m0_9, m10_19, m20_29},
		},
		{
			input: []*RangeMetadata{m0_9, m10_19, m20_29, m50_59},
			want:  []*RangeMetadata{m0_9, m10_19, m20_29},
		},
		{
			input: []*RangeMetadata{m0_9, m20_29, m50_59},
			want:  []*RangeMetadata{m0_9},
		},
		{
			input: []*RangeMetadata{m0_29, m20_29, m30_39},
			want:  []*RangeMetadata{m0_29, m30_39},
		},
		{
			input: []*RangeMetadata{m0_9, m0_29, m10_19, m30_39},
			want:  []*RangeMetadata{m0_29, m30_39},
		},
		{
			input: []*RangeMetadata{m0_9, m0_29, m10_59, m30_39},
			want:  []*RangeMetadata{m0_9, m10_59},
		},
		{
			input: []*RangeMetadata{m0_9, m0_9, m0_29, m10_59, m30_39},
			want:  []*RangeMetadata{m0_9, m10_59},
		},
		{
			// two equivalent sequences, shorter one wins
			input: []*RangeMetadata{m10_59, m30_39, m50_59, m40_49, m0_9, m0_29},
			want:  []*RangeMetadata{m0_9, m10_59},
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, findLongestRangeCheckpoint(tc.input))
	}
}

func newEpochRangeMetadataForTesting(minEpoch, maxEpoch int) *RangeMetadata {
	return &RangeMetadata{MinEpoch: minEpoch, MaxEpoch: maxEpoch}
}
