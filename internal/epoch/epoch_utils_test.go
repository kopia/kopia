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

func TestGetKeyRange(t *testing.T) {
	cases := []struct {
		input     map[int]bool
		want      intRange
		shouldErr bool
		length    uint
	}{
		{},
		{
			input:  map[int]bool{-5: true},
			want:   intRange{lo: -5, hi: -4},
			length: 1,
		},
		{
			input:  map[int]bool{-5: true, -4: true},
			want:   intRange{lo: -5, hi: -3},
			length: 2,
		},
		{
			input:  map[int]bool{0: true},
			want:   intRange{lo: 0, hi: 1},
			length: 1,
		},
		{
			input:  map[int]bool{5: true},
			want:   intRange{lo: 5, hi: 6},
			length: 1,
		},
		{
			input:  map[int]bool{0: true, 1: true},
			want:   intRange{lo: 0, hi: 2},
			length: 2,
		},
		{
			input:  map[int]bool{8: true, 9: true},
			want:   intRange{lo: 8, hi: 10},
			length: 2,
		},
		{
			input:  map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true},
			want:   intRange{lo: 1, hi: 6},
			length: 5,
		},
		{
			input:     map[int]bool{8: true, 10: true},
			want:      intRange{},
			shouldErr: true,
		},
		{
			input:     map[int]bool{1: true, 2: true, 3: true, 5: true},
			want:      intRange{},
			shouldErr: true,
		},
		{
			input:     map[int]bool{-5: true, -7: true},
			want:      intRange{},
			shouldErr: true,
		},
		{
			input:     map[int]bool{0: true, minInt: true},
			want:      intRange{},
			shouldErr: true,
		},
		{
			input:     map[int]bool{0: true, maxInt: true},
			want:      intRange{},
			shouldErr: true,
		},
		{
			input:     map[int]bool{maxInt: true, minInt: true},
			want:      intRange{},
			shouldErr: true,
		},
		{
			input:  map[int]bool{minInt: true},
			want:   intRange{lo: minInt, hi: minInt + 1},
			length: 1,
		},
		{
			input:  map[int]bool{maxInt - 1: true},
			want:   intRange{lo: maxInt - 1, hi: maxInt},
			length: 1,
		},
		{
			input:  map[int]bool{maxInt: true},
			want:   intRange{lo: maxInt, hi: minInt}, // not representable corner case :(
			length: 1,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprint("case: ", i), func(t *testing.T) {
			got, err := getKeyRange(tc.input)
			if tc.shouldErr {
				require.Error(t, err, "input: %v", tc.input)
			}

			require.Equal(t, tc.want, got, "input: %#v", tc.input)
			require.Equal(t, tc.length, got.length())
		})
	}
}

func TestAssertMinMaxIntConstants(t *testing.T) {
	require.Equal(t, math.MinInt, minInt)
	require.Equal(t, math.MaxInt, maxInt)
}
