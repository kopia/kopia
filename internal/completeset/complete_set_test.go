package completeset_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/completeset"
	"github.com/kopia/kopia/repo/blob"
)

func TestFindFirst(t *testing.T) {
	cases := []struct {
		input []blob.ID
		want  []blob.ID
	}{
		{
			input: []blob.ID{},
			want:  []blob.ID{},
		},

		// one complete session of size 2
		{
			input: []blob.ID{
				"a-s0-c2",
				"b-s0-c2",
			},
			want: []blob.ID{
				"a-s0-c2",
				"b-s0-c2",
			},
		},
		// one complete session with some malformed name
		{
			input: []blob.ID{
				"a-s0-c2",
				"malformed",
				"b-s0-c2",
			},
			want: []blob.ID{
				"a-s0-c2",
				"b-s0-c2",
			},
		},
		// one complete session with some malformed blob ID
		{
			input: []blob.ID{
				"a-s0-c2",
				"malformed-s0-x2",
				"b-s0-c2",
			},
			want: []blob.ID{
				"a-s0-c2",
				"b-s0-c2",
			},
		},
		// one complete session with some malformed count
		{
			input: []blob.ID{
				"a-s0-c2",
				"malformed-s0-cNAN",
				"b-s0-c2",
			},
			want: []blob.ID{
				"a-s0-c2",
				"b-s0-c2",
			},
		},
		// two complete sessions, we pick 's0' as it's the first one to become complete.
		{
			input: []blob.ID{
				"foo-s0-c2",
				"aaa-s1-c2",
				"bar-s0-c2",
				"bbb-s1-c2",
			},
			want: []blob.ID{
				"foo-s0-c2",
				"bar-s0-c2",
			},
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, blob.IDsFromMetadata(completeset.FindFirst(dummyMetadataForIDs(tc.input))), "invalid result for %v", tc.input)
	}
}

func dummyMetadataForIDs(ids []blob.ID) []blob.Metadata {
	var result []blob.Metadata

	for _, id := range ids {
		result = append(result, blob.Metadata{BlobID: id})
	}

	return result
}
