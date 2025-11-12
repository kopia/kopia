package completeset_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/completeset"
	"github.com/kopia/kopia/repo/blob"
)

func TestFindFirstAndAll(t *testing.T) {
	cases := []struct {
		input                 []blob.ID
		wantFirst             []blob.ID
		wantAll               [][]blob.ID
		wantExcludeIncomplete []blob.ID
	}{
		{
			input:                 []blob.ID{},
			wantFirst:             []blob.ID{},
			wantAll:               [][]blob.ID{},
			wantExcludeIncomplete: []blob.ID{},
		},

		// one complete session of size 2
		{
			input: []blob.ID{
				"a-s0-c2",
				"b-s0-c2",
			},
			wantFirst: []blob.ID{"a-s0-c2", "b-s0-c2"},
			wantAll: [][]blob.ID{
				{"a-s0-c2", "b-s0-c2"},
			},
			wantExcludeIncomplete: []blob.ID{"a-s0-c2", "b-s0-c2"},
		},
		// one complete session with some malformed name, which by itself forms a complete session.
		{
			input: []blob.ID{
				"a-s0-c2",
				"malformed",
				"b-s0-c2",
			},
			wantFirst: []blob.ID{
				"malformed",
			},
			wantAll: [][]blob.ID{
				{"malformed"},
				{"a-s0-c2", "b-s0-c2"},
			},
			wantExcludeIncomplete: []blob.ID{
				"malformed",
				"a-s0-c2", "b-s0-c2",
			},
		},
		// one complete session with some malformed blob ID
		{
			input: []blob.ID{
				"a-s0-c2",
				"malformed-s0-x2",
				"b-s0-c2",
			},
			wantFirst: []blob.ID{
				"malformed-s0-x2",
			},
			wantAll: [][]blob.ID{
				{"malformed-s0-x2"},
				{"a-s0-c2", "b-s0-c2"},
			},
			wantExcludeIncomplete: []blob.ID{"malformed-s0-x2", "a-s0-c2", "b-s0-c2"},
		},
		// one complete session with some malformed count
		{
			input: []blob.ID{
				"a-s0-c2",
				"malformed-s0-cNAN",
				"b-s0-c2",
			},
			wantFirst: []blob.ID{
				"malformed-s0-cNAN",
			},
			wantAll: [][]blob.ID{
				{"malformed-s0-cNAN"},
				{"a-s0-c2", "b-s0-c2"},
			},
			wantExcludeIncomplete: []blob.ID{
				"malformed-s0-cNAN",
				"a-s0-c2", "b-s0-c2",
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
			wantFirst: []blob.ID{
				"foo-s0-c2", "bar-s0-c2",
			},
			wantAll: [][]blob.ID{
				{"foo-s0-c2", "bar-s0-c2"},
				{"aaa-s1-c2", "bbb-s1-c2"},
			},
			wantExcludeIncomplete: []blob.ID{
				"foo-s0-c2", "bar-s0-c2",
				"aaa-s1-c2", "bbb-s1-c2",
			},
		},
		// two incomplete sessions
		{
			input: []blob.ID{
				"foo-s0-c3",
				"aaa-s1-c3",
				"bar-s0-c3",
				"bbb-s1-c3",
			},
			wantFirst:             []blob.ID{},
			wantAll:               [][]blob.ID{},
			wantExcludeIncomplete: []blob.ID{},
		},
		// two complete, two incomplete sessions
		{
			input: []blob.ID{
				"foo-s0-c2",
				"aaa-s1-c3",
				"bar-s0-c2",
				"bbb-s1-c3",
				"foo-s2-c2",
				"aaa-s3-c3",
				"bar-s2-c2",
				"bbb-s3-c3",
			},
			wantFirst: []blob.ID{
				"foo-s0-c2",
				"bar-s0-c2",
			},
			wantAll: [][]blob.ID{
				{"foo-s0-c2", "bar-s0-c2"},
				{"foo-s2-c2", "bar-s2-c2"},
			},
			wantExcludeIncomplete: []blob.ID{
				"foo-s0-c2", "bar-s0-c2", "foo-s2-c2", "bar-s2-c2",
			},
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.wantFirst, blob.IDsFromMetadata(completeset.FindFirst(dummyMetadataForIDs(tc.input))), "invalid result for FindFirst(%v)", tc.input)
		require.Equal(t, tc.wantAll, idsFromMetadataSets(completeset.FindAll(dummyMetadataForIDs(tc.input))), "invalid result for FindAll(%v)", tc.input)
		require.Equal(t, tc.wantExcludeIncomplete, blob.IDsFromMetadata(completeset.ExcludeIncomplete(dummyMetadataForIDs(tc.input))), "invalid result for ExcludeIncomplete(%v)", tc.input)
	}
}

func idsFromMetadataSets(sets [][]blob.Metadata) [][]blob.ID {
	result := make([][]blob.ID, 0, len(sets))

	for _, s := range sets {
		result = append(result, blob.IDsFromMetadata(s))
	}

	return result
}

func dummyMetadataForIDs(ids []blob.ID) []blob.Metadata {
	result := make([]blob.Metadata, 0, len(ids))

	for _, id := range ids {
		result = append(result, blob.Metadata{BlobID: id})
	}

	return result
}
