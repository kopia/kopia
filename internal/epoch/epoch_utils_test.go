package epoch

import (
	"fmt"
	"math"
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

func TestGetFirstContiguousKeyRange(t *testing.T) {
	cases := []struct {
		input   map[int]bool
		want    closedIntRange
		length  uint
		isEmpty bool
	}{
		{
			isEmpty: true,
			want:    closedIntRange{0, -1},
		},
		{
			input:  map[int]bool{0: true},
			want:   closedIntRange{lo: 0, hi: 0},
			length: 1,
		},
		{
			input:  map[int]bool{-5: true},
			want:   closedIntRange{lo: -5, hi: -5},
			length: 1,
		},
		{
			input:  map[int]bool{-5: true, -4: true},
			want:   closedIntRange{lo: -5, hi: -4},
			length: 2,
		},
		{
			input:  map[int]bool{0: true},
			want:   closedIntRange{lo: 0, hi: 0},
			length: 1,
		},
		{
			input:  map[int]bool{5: true},
			want:   closedIntRange{lo: 5, hi: 5},
			length: 1,
		},
		{
			input:  map[int]bool{0: true, 1: true},
			want:   closedIntRange{lo: 0, hi: 1},
			length: 2,
		},
		{
			input:  map[int]bool{8: true, 9: true},
			want:   closedIntRange{lo: 8, hi: 9},
			length: 2,
		},
		{
			input:  map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true},
			want:   closedIntRange{lo: 1, hi: 5},
			length: 5,
		},
		{
			input:  map[int]bool{8: true, 10: true},
			want:   closedIntRange{lo: 8, hi: 8},
			length: 1,
		},
		{
			input:  map[int]bool{1: true, 2: true, 3: true, 5: true},
			want:   closedIntRange{lo: 1, hi: 3},
			length: 3,
		},
		{
			input:  map[int]bool{-5: true, -7: true},
			want:   closedIntRange{lo: -7, hi: -7},
			length: 1,
		},
		{
			input:  map[int]bool{0: true, minInt: true},
			want:   closedIntRange{lo: minInt, hi: minInt},
			length: 1,
		},
		{
			input:  map[int]bool{0: true, maxInt: true},
			want:   closedIntRange{lo: 0, hi: 0},
			length: 1,
		},
		{
			input:  map[int]bool{maxInt: true, minInt: true},
			want:   closedIntRange{lo: minInt, hi: minInt},
			length: 1,
		},
		{
			input:  map[int]bool{minInt: true},
			want:   closedIntRange{lo: minInt, hi: minInt},
			length: 1,
		},
		{
			input:  map[int]bool{maxInt - 1: true},
			want:   closedIntRange{lo: maxInt - 1, hi: maxInt - 1},
			length: 1,
		},
		{
			input:  map[int]bool{maxInt: true},
			want:   closedIntRange{lo: maxInt, hi: maxInt},
			length: 1,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprint("case:", i), func(t *testing.T) {
			got := getFirstContiguousKeyRange(tc.input)

			require.Equal(t, tc.want, got, "input: %#v", tc.input)
			require.Equal(t, tc.length, got.length())
			require.Equal(t, tc.isEmpty, got.isEmpty())
		})
	}
}
