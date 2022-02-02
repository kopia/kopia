package epoch

import (
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
