package index

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/blob"
)

func TestMerged(t *testing.T) {
	i1, err := indexWithItems(
		Info{ContentID: mustParseID(t, "aabbcc"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 11},
		Info{ContentID: mustParseID(t, "ddeeff"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 111},
		Info{ContentID: mustParseID(t, "z010203"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 111},
		Info{ContentID: mustParseID(t, "de1e1e"), TimestampSeconds: 4, PackBlobID: "xx", PackOffset: 111},
	)
	require.NoError(t, err)

	i2, err := indexWithItems(
		Info{ContentID: mustParseID(t, "aabbcc"), TimestampSeconds: 3, PackBlobID: "yy", PackOffset: 33},
		Info{ContentID: mustParseID(t, "xaabbcc"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 111},
		Info{ContentID: mustParseID(t, "de1e1e"), TimestampSeconds: 4, PackBlobID: "xx", PackOffset: 222, Deleted: true},
	)
	require.NoError(t, err)

	i3, err := indexWithItems(
		Info{ContentID: mustParseID(t, "aabbcc"), TimestampSeconds: 2, PackBlobID: "zz", PackOffset: 22},
		Info{ContentID: mustParseID(t, "ddeeff"), TimestampSeconds: 1, PackBlobID: "zz", PackOffset: 222},
		Info{ContentID: mustParseID(t, "k010203"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 111},
		Info{ContentID: mustParseID(t, "k020304"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 111},
	)
	require.NoError(t, err)

	m := Merged{i1, i2, i3}

	require.Equal(t, 11, m.ApproximateCount())

	var i Info

	ok, err := m.GetInfo(mustParseID(t, "aabbcc"), &i)
	require.True(t, ok)
	require.NoError(t, err)

	require.Equal(t, uint32(33), i.PackOffset)

	require.NoError(t, m.Iterate(AllIDs, func(i Info) error {
		if i.ContentID == mustParseID(t, "de1e1e") {
			if i.Deleted {
				t.Errorf("iteration preferred deleted content over non-deleted")
			}
		}
		return nil
	}))

	fmt.Println("=========== START")

	// error is propagated.
	someErr := errors.New("some error")
	require.ErrorIs(t, m.Iterate(AllIDs, func(i Info) error {
		if i.ContentID == mustParseID(t, "aabbcc") {
			return someErr
		}

		return nil
	}), someErr)

	fmt.Println("=========== END")

	// empty merged index does not invoke callback during iteration.
	require.NoError(t, Merged{}.Iterate(AllIDs, func(i Info) error {
		return someErr
	}))

	ok, err = m.GetInfo(mustParseID(t, "de1e1e"), &i)
	require.True(t, ok)
	require.NoError(t, err)
	require.False(t, i.Deleted)

	cases := []struct {
		r IDRange

		wantIDs []ID
	}{
		{
			r: AllIDs,
			wantIDs: []ID{
				mustParseID(t, "aabbcc"),
				mustParseID(t, "ddeeff"),
				mustParseID(t, "de1e1e"),
				mustParseID(t, "k010203"),
				mustParseID(t, "k020304"),
				mustParseID(t, "xaabbcc"),
				mustParseID(t, "z010203"),
			},
		},
		{
			r: AllNonPrefixedIDs,
			wantIDs: []ID{
				mustParseID(t, "aabbcc"),
				mustParseID(t, "ddeeff"),
				mustParseID(t, "de1e1e"),
			},
		},
		{
			r: AllPrefixedIDs,
			wantIDs: []ID{
				mustParseID(t, "k010203"),
				mustParseID(t, "k020304"),
				mustParseID(t, "xaabbcc"),
				mustParseID(t, "z010203"),
			},
		},
		{
			r: IDRange{"a", "e"},
			wantIDs: []ID{
				mustParseID(t, "aabbcc"),
				mustParseID(t, "ddeeff"),
				mustParseID(t, "de1e1e"),
			},
		},
		{
			r: PrefixRange("dd"),
			wantIDs: []ID{
				mustParseID(t, "ddeeff"),
			},
		},
		{
			r: IDRange{"dd", "df"},
			wantIDs: []ID{
				mustParseID(t, "ddeeff"),
				mustParseID(t, "de1e1e"),
			},
		},
	}

	for _, tc := range cases {
		inOrder := iterateIDRange(t, m, tc.r)
		if !reflect.DeepEqual(inOrder, tc.wantIDs) {
			t.Errorf("unexpected items in order: %v, wanted %v", inOrder, tc.wantIDs)
		}
	}

	if err := m.Close(); err != nil {
		t.Errorf("unexpected error in Close(): %v", err)
	}
}

type failingIndex struct {
	Index
	err error
}

func (i failingIndex) GetInfo(contentID ID, result *Info) (bool, error) {
	return false, i.err
}

func TestMergedGetInfoError(t *testing.T) {
	someError := errors.New("some error")

	m := Merged{failingIndex{nil, someError}}

	var info Info
	ok, err := m.GetInfo(mustParseID(t, "xabcdef"), &info)
	require.ErrorIs(t, err, someError)
	require.False(t, ok)
}

func TestMergedIndexIsConsistent(t *testing.T) {
	i1, err := indexWithItems(
		Info{ContentID: mustParseID(t, "aabbcc"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 11},
		Info{ContentID: mustParseID(t, "bbccdd"), TimestampSeconds: 1, PackBlobID: "xx", PackOffset: 11},
		Info{ContentID: mustParseID(t, "ccddee"), TimestampSeconds: 1, PackBlobID: "ff", PackOffset: 11, Deleted: true},
	)
	require.NoError(t, err)

	i2, err := indexWithItems(
		Info{ContentID: mustParseID(t, "aabbcc"), TimestampSeconds: 1, PackBlobID: "yy", PackOffset: 33},
		Info{ContentID: mustParseID(t, "bbccdd"), TimestampSeconds: 1, PackBlobID: "yy", PackOffset: 11, Deleted: true},
		Info{ContentID: mustParseID(t, "ccddee"), TimestampSeconds: 1, PackBlobID: "gg", PackOffset: 11, Deleted: true},
	)
	require.NoError(t, err)

	i3, err := indexWithItems(
		Info{ContentID: mustParseID(t, "aabbcc"), TimestampSeconds: 1, PackBlobID: "zz", PackOffset: 22},
		Info{ContentID: mustParseID(t, "bbccdd"), TimestampSeconds: 1, PackBlobID: "zz", PackOffset: 11, Deleted: true},
		Info{ContentID: mustParseID(t, "ccddee"), TimestampSeconds: 1, PackBlobID: "hh", PackOffset: 11, Deleted: true},
	)
	require.NoError(t, err)

	cases := []Merged{
		{i1, i2, i3},
		{i1, i3, i2},
		{i2, i1, i3},
		{i2, i3, i1},
		{i3, i1, i2},
		{i3, i2, i1},
	}

	for _, m := range cases {
		var i Info

		ok, err := m.GetInfo(mustParseID(t, "aabbcc"), &i)
		if err != nil || !ok {
			t.Fatalf("unable to get info: %v", err)
		}

		// all things being equal, highest pack blob ID wins
		require.Equal(t, blob.ID("zz"), i.PackBlobID)

		ok, err = m.GetInfo(mustParseID(t, "bbccdd"), &i)
		if err != nil || !ok {
			t.Fatalf("unable to get info: %v", err)
		}

		// given identical timestamps, non-deleted wins.
		require.Equal(t, blob.ID("xx"), i.PackBlobID)

		ok, err = m.GetInfo(mustParseID(t, "ccddee"), &i)
		if err != nil || !ok {
			t.Fatalf("unable to get info: %v", err)
		}

		// given identical timestamps and all deleted, highest pack blob ID wins.
		require.Equal(t, blob.ID("hh"), i.PackBlobID)
	}
}

func iterateIDRange(t *testing.T, m Index, r IDRange) []ID {
	t.Helper()

	var inOrder []ID

	require.NoError(t, m.Iterate(r, func(i Info) error {
		inOrder = append(inOrder, i.ContentID)
		return nil
	}))

	return inOrder
}

func indexWithItems(items ...Info) (Index, error) {
	b := make(Builder)

	for _, it := range items {
		b.Add(it)
	}

	var buf bytes.Buffer
	if err := b.Build(&buf, Version2); err != nil {
		return nil, errors.Wrap(err, "build error")
	}

	return Open(buf.Bytes(), nil, func() int { return fakeEncryptionOverhead })
}
