package epoch

import (
	"fmt"
	"math"
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

func TestGetContiguosKeyRange(t *testing.T) {
	invalidEmptyRange := closedIntRange{-1, -2}

	cases := []struct {
		input     map[int]bool
		want      closedIntRange
		shouldErr bool
		length    uint
		isEmpty   bool
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
			input:     map[int]bool{8: true, 10: true},
			want:      invalidEmptyRange,
			shouldErr: true,
			isEmpty:   true,
		},
		{
			input:     map[int]bool{1: true, 2: true, 3: true, 5: true},
			want:      invalidEmptyRange,
			shouldErr: true,
			isEmpty:   true,
		},
		{
			input:     map[int]bool{-5: true, -7: true},
			want:      invalidEmptyRange,
			shouldErr: true,
			isEmpty:   true,
		},
		{
			input:     map[int]bool{0: true, minInt: true},
			want:      invalidEmptyRange,
			shouldErr: true,
			isEmpty:   true,
		},
		{
			input:     map[int]bool{0: true, maxInt: true},
			want:      invalidEmptyRange,
			shouldErr: true,
			isEmpty:   true,
		},
		{
			input:     map[int]bool{maxInt: true, minInt: true},
			want:      invalidEmptyRange,
			shouldErr: true,
			isEmpty:   true,
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
		t.Run(fmt.Sprint("case: ", i), func(t *testing.T) {
			got, err := getContiguousKeyRange(tc.input)
			if tc.shouldErr {
				require.Error(t, err, "input: %v", tc.input)
			}

			require.Equal(t, tc.want, got, "input: %#v", tc.input)
			require.Equal(t, tc.length, got.length())
			require.Equal(t, tc.isEmpty, got.isEmpty())
		})
	}
}

func TestAssertMinMaxIntConstants(t *testing.T) {
	require.Equal(t, math.MinInt, minInt)
	require.Equal(t, math.MaxInt, maxInt)
}

func TestGetKeyRange(t *testing.T) {
	cases := []struct {
		input   map[int]bool
		want    closedIntRange
		length  uint
		isEmpty bool
	}{
		{
			isEmpty: true,
			want:    closedIntRange{lo: 0, hi: -1},
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
			want:   closedIntRange{lo: 8, hi: 10},
			length: 3,
		},
		{
			input:  map[int]bool{1: true, 2: true, 3: true, 5: true},
			want:   closedIntRange{lo: 1, hi: 5},
			length: 5,
		},
		{
			input:  map[int]bool{-5: true, -7: true},
			want:   closedIntRange{lo: -7, hi: -5},
			length: 3,
		},
		{
			input:  map[int]bool{0: true, minInt: true},
			want:   closedIntRange{lo: minInt, hi: 0},
			length: -minInt + 1,
		},
		{
			input:  map[int]bool{0: true, maxInt: true},
			want:   closedIntRange{lo: 0, hi: maxInt},
			length: maxInt + 1,
		},
		{
			input:   map[int]bool{maxInt: true, minInt: true},
			want:    closedIntRange{lo: minInt, hi: maxInt},
			length:  0, // corner case, not representable :(
			isEmpty: true,
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
		t.Run(fmt.Sprint("case: ", i), func(t *testing.T) {
			got := getKeyRange(tc.input)

			require.Equal(t, tc.want, got, "input: %#v", tc.input)
			require.Equal(t, tc.length, got.length())
			require.Equal(t, tc.isEmpty, got.isEmpty())
		})
	}
}

func TestOldestUncompactedEpoch(t *testing.T) {
	cases := []struct {
		input         CurrentSnapshot
		expectedEpoch int
		wantErr       error
	}{
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: map[int][]blob.Metadata{},
			},
		},
		{
			input: CurrentSnapshot{
				WriteEpoch: 0,
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					0: {blob.Metadata{BlobID: compactedEpochBlobPrefix(0) + "foo0"}},
				},
			},
			expectedEpoch: 1,
		},
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					0: {blob.Metadata{BlobID: compactedEpochBlobPrefix(0) + "foo0"}},
					1: {blob.Metadata{BlobID: compactedEpochBlobPrefix(1) + "foo1"}},
				},
			},
			expectedEpoch: 2,
		},
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					1: {blob.Metadata{BlobID: compactedEpochBlobPrefix(1) + "foo1"}},
				},
			},
			expectedEpoch: 0,
		},
		{
			input: CurrentSnapshot{
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					0: {blob.Metadata{BlobID: compactedEpochBlobPrefix(0) + "foo0"}},
					2: {blob.Metadata{BlobID: compactedEpochBlobPrefix(2) + "foo2"}},
				},
			},
			expectedEpoch: -1,
			wantErr:       errNonContiguousRange,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					0: {blob.Metadata{BlobID: compactedEpochBlobPrefix(0) + "foo0"}},
					1: {blob.Metadata{BlobID: compactedEpochBlobPrefix(1) + "foo1"}},
				},
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					1: {blob.Metadata{BlobID: compactedEpochBlobPrefix(1) + "foo1"}},
					2: {blob.Metadata{BlobID: compactedEpochBlobPrefix(2) + "foo2"}},
				},
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					1: {blob.Metadata{BlobID: compactedEpochBlobPrefix(1) + "foo1"}},
				},
			},
			expectedEpoch: 3,
		},

		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					4: {blob.Metadata{BlobID: compactedEpochBlobPrefix(4) + "foo4"}},
					5: {blob.Metadata{BlobID: compactedEpochBlobPrefix(5) + "foo5"}},
				},
			},
			expectedEpoch: 3,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					2: {blob.Metadata{BlobID: compactedEpochBlobPrefix(2) + "foo2"}},
					3: {blob.Metadata{BlobID: compactedEpochBlobPrefix(3) + "foo3"}},
				},
			},
			expectedEpoch: 4,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					3: {blob.Metadata{BlobID: compactedEpochBlobPrefix(3) + "foo3"}},
					4: {blob.Metadata{BlobID: compactedEpochBlobPrefix(4) + "foo4"}},
				},
			},
			expectedEpoch: 5,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 1,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(1, 2) + "foo-1-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					3: {blob.Metadata{BlobID: compactedEpochBlobPrefix(3) + "foo3"}},
					4: {blob.Metadata{BlobID: compactedEpochBlobPrefix(4) + "foo4"}},
				},
			},
			expectedEpoch: -1,
			wantErr:       errInvalidCompactedRange,
		},
		{
			input: CurrentSnapshot{
				LongestRangeCheckpointSets: []*RangeMetadata{
					{
						MinEpoch: 0,
						MaxEpoch: 2,
						Blobs: []blob.Metadata{
							{BlobID: rangeCheckpointBlobPrefix(0, 2) + "foo-0-2"},
						},
					},
				},
				SingleEpochCompactionSets: map[int][]blob.Metadata{
					3: {blob.Metadata{BlobID: compactedEpochBlobPrefix(3) + "foo3"}},
					5: {blob.Metadata{BlobID: compactedEpochBlobPrefix(5) + "foo5"}},
				},
			},
			expectedEpoch: -1,
			wantErr:       errNonContiguousRange,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprint("case: ", i), func(t *testing.T) {
			got, err := oldestUncompactedEpoch(tc.input)

			if tc.wantErr != nil {
				require.Error(t, err)
			}

			require.Equal(t, tc.expectedEpoch, got, "input: %#v", tc.input)
		})
	}
}
