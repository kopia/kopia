package epoch

import (
	"testing"
	"time"

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

func TestBlobsWrittenBefore(t *testing.T) {
	t0 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	bm0 := blob.Metadata{BlobID: "a", Timestamp: t0}

	t1 := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)
	bm1 := blob.Metadata{BlobID: "b", Timestamp: t1}

	t2 := time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC)
	bm2 := blob.Metadata{BlobID: "c", Timestamp: t2}

	cases := []struct {
		bms    []blob.Metadata
		cutoff time.Time
		want   []blob.Metadata
	}{
		{[]blob.Metadata{bm0, bm1, bm2}, time.Time{}, []blob.Metadata{bm0, bm1, bm2}},
		{[]blob.Metadata{bm0, bm1, bm2}, t0.Add(-1), nil},
		{[]blob.Metadata{bm0, bm1, bm2}, t0, []blob.Metadata{bm0}},
		{[]blob.Metadata{bm0, bm1, bm2}, t1.Add(-1), []blob.Metadata{bm0}},
		{[]blob.Metadata{bm0, bm1, bm2}, t1, []blob.Metadata{bm0, bm1}},
		{[]blob.Metadata{bm0, bm1, bm2}, t2.Add(-1), []blob.Metadata{bm0, bm1}},
		{[]blob.Metadata{bm0, bm1, bm2}, t2, []blob.Metadata{bm0, bm1, bm2}},
	}

	for i, tc := range cases {
		require.Equal(t, tc.want, blobsWrittenBefore(tc.bms, tc.cutoff), "#%v blobsWrittenBefore(%v,%v)", i, tc.bms, tc.cutoff)
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
