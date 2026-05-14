package epoch

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/blob"
)

func TestEpochNumberFromBlobID(t *testing.T) {
	cases := []struct {
		input blob.ID
		want  int
	}{
		{"pppp9", 9},
		{"x7", 7},
		{"x01234_1235", 1234},
		{"x0_1235", 0},
		{"abc01234_", 1234},
		{"abc1234_", 1234},
		{"abc1234_xxxx-sadfasd", 1234},
	}

	for _, tc := range cases {
		n, ok := epochNumberFromBlobID(tc.input)
		require.True(t, ok, tc.input)
		require.Equal(t, tc.want, n)
	}
}

func TestEpochNumberFromBlobID_Invalid(t *testing.T) {
	cases := []blob.ID{
		"_",
		"a_",
		"x123x_",
	}

	for _, tc := range cases {
		_, ok := epochNumberFromBlobID(tc)
		require.False(t, ok, "epochNumberFromBlobID(%v)", tc)
	}
}

func TestGroupByEpochNumber(t *testing.T) {
	cases := []struct {
		input []blob.Metadata
		want  map[int][]blob.Metadata
	}{
		{
			input: []blob.Metadata{
				{BlobID: "e1_abc"},
				{BlobID: "e2_cde"},
				{BlobID: "e1_def"},
				{BlobID: "e3_fgh"},
			},
			want: map[int][]blob.Metadata{
				1: {
					{BlobID: "e1_abc"},
					{BlobID: "e1_def"},
				},
				2: {
					{BlobID: "e2_cde"},
				},
				3: {
					{BlobID: "e3_fgh"},
				},
			},
		},
	}

	for _, tc := range cases {
		got := groupByEpochNumber(tc.input)
		require.Equal(t, tc.want, got)
	}
}

func TestAssertMinMaxIntConstants(t *testing.T) {
	require.Equal(t, math.MinInt, minInt)
	require.Equal(t, math.MaxInt, maxInt)
}

func TestOldestUncompactedEpoch(t *testing.T) {
	cases := []struct {
		input         CurrentSnapshot
		expectedEpoch int
		wantErr       error
	}{
		// cases with non-contiguous single epoch compaction sets are needed for
		// compatibility with older clients.
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: map[int][]blob.Metadata{},
			},
			expectedEpoch: 0,
		},
		{
			input: CurrentSnapshot{
				WriteEpoch:                0,
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0}),
			},
			expectedEpoch: 1,
		},
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1}),
			},
			expectedEpoch: 2,
		},
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{1}),
			},
			expectedEpoch: 0,
		},
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{23}),
			},
			expectedEpoch: 0,
		},
		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 2}),
			},
			expectedEpoch: 1,
		},
		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 4}),
			},
			expectedEpoch: 1,
		},

		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 3}),
			},
			expectedEpoch: 2,
		},

		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 4}),
			},
			expectedEpoch: 2,
		},
		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 4, 5}),
			},
			expectedEpoch: 2,
		},
		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 2, 4}),
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 2, 4, 6, 9}),
			},
			expectedEpoch: 3,
		},

		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{0, 1}),
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{1, 2}),
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{1}),
			},
			expectedEpoch: 3,
		},

		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{4, 5}),
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{2, 3}),
			},
			expectedEpoch: 4,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{3, 4}),
			},
			expectedEpoch: 5,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(1, 2),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{3, 4}),
			},
			expectedEpoch: -1,
			wantErr:       errInvalidCompactedRange,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 2),
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{3, 5}),
			},
			expectedEpoch: 4,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				// non-contiguous single epoch compaction set, but most of the set overlaps with the compacted range
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 2, 4, 6, 9}),
			},
			expectedEpoch: 8,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				// non-contiguous single epoch compaction set, the first contiguous sequence fully overlaps with the compacted range
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 2, 4, 6, 7, 9}),
			},
			expectedEpoch: 8,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				// non-contiguous single epoch compaction set, but most of the
				// set overlaps with the compacted range except for the last
				// epoch in the range (7), and the next epoch (8) is in the
				// single compaction set already
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 2, 4, 6, 8, 9}),
			},
			expectedEpoch: 10,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				// non-contiguous single epoch compaction set, but most of the
				// set overlaps with the compacted range except for the last
				// epoch in the range (7), and the next epoch (8) is in the
				// single compaction set already
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{0, 1, 2, 4, 6, 8, 10}),
			},
			expectedEpoch: 9,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				SingleEpochCompactionSets:  makeSingleCompactionEpochSets([]int{9, 10}),
			},
			expectedEpoch: 8,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{8, 10}),
			},
			expectedEpoch: 9,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: makeLongestRange(0, 7),
				// non-contiguous single epoch compaction set
				SingleEpochCompactionSets: makeSingleCompactionEpochSets([]int{8, 9, 12}),
			},
			expectedEpoch: 10,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprint("case:", i), func(t *testing.T) {
			got, err := oldestUncompactedEpoch(tc.input)

			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}

			require.Equal(t, tc.expectedEpoch, got, "input: %#v", tc.input)
		})
	}
}

func makeSingleCompactionEpochSets(epochs []int) map[int][]blob.Metadata {
	es := make(map[int][]blob.Metadata, len(epochs))
	for _, e := range epochs {
		es[e] = []blob.Metadata{{BlobID: compactedEpochBlobPrefix(e) + "foo_" + blob.ID(strconv.Itoa(e))}}
	}

	return es
}

func makeLongestRange(minEpoch, maxEpoch int) []*RangeMetadata {
	return []*RangeMetadata{
		{
			MinEpoch: minEpoch,
			MaxEpoch: maxEpoch,
			Blobs: []blob.Metadata{
				{BlobID: blob.ID(fmt.Sprintf("%sfoo-%v-%v", rangeCheckpointBlobPrefix(minEpoch, maxEpoch), minEpoch, maxEpoch))},
			},
		},
	}
}

func TestGetOldestUncompactedAfterEpoch(t *testing.T) {
	cases := []struct {
		in        []int
		threshold int
		expected  int
	}{
		{},
		{
			threshold: 5,
			expected:  5,
		},
		{
			in:        []int{},
			threshold: 0,
			expected:  0,
		},
		{
			in:        []int{0},
			threshold: 0,
			expected:  1,
		},

		{
			in:        []int{0},
			threshold: 1,
			expected:  1,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 0,
			expected:  1,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 1,
			expected:  1,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 2,
			expected:  4,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 3,
			expected:  4,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 4,
			expected:  4,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 5,
			expected:  6,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 6,
			expected:  6,
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 8,
			expected:  8,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 0,
			expected:  2,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 1,
			expected:  2,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 2,
			expected:  2,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 3,
			expected:  3,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 4,
			expected:  6,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 5,
			expected:  6,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 6,
			expected:  6,
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 7,
			expected:  7,
		},
	}

	for i, tc := range cases {
		t.Run("case:"+strconv.Itoa(i), func(t *testing.T) {
			vseq := slices.Values(tc.in)
			got := getOldestUncompactedAfterEpoch(vseq, tc.threshold)

			require.Equal(t, tc.expected, got)
		})
	}
}

func TestFilterLowerThan(t *testing.T) {
	cases := []struct {
		in        []int
		threshold int
		expected  []int
	}{
		{},
		{
			threshold: 5,
		},
		{
			in:        []int{},
			threshold: 0,
			expected:  []int{},
		},
		{
			in:        []int{0},
			threshold: 0,
			expected:  []int{0},
		},
		{
			in:        []int{0},
			threshold: 1,
			expected:  []int{},
		},
		{
			in:        []int{0, 2, 5, 3},
			threshold: 6,
			expected:  []int{},
		},
		{
			in:        []int{1, 0, 5, 4},
			threshold: 0,
			expected:  []int{1, 0, 5, 4},
		},
		{
			in:        []int{1, 0, -1, 5, 4},
			threshold: 3,
			expected:  []int{4, 5},
		},
		{
			in:        []int{1, 0, -1, 5, 4},
			threshold: 4,
			expected:  []int{4, 5},
		},
	}

	for i, tc := range cases {
		t.Run("case:"+strconv.Itoa(i), func(t *testing.T) {
			vseq := slices.Values(tc.in)
			got := filterLowerThan(tc.threshold, vseq)
			gotSlice := slices.Collect(got)

			require.Subset(t, tc.in, gotSlice)
			require.ElementsMatch(t, gotSlice, tc.expected)
		})
	}
}
