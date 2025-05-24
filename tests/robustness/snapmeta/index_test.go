//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	idx := Index{}

	const (
		snapshotIndexName = "snapshotIndex"
		snapIDKey         = "snapID1"
	)

	idx.AddToIndex(snapIDKey, snapshotIndexName)

	keys := idx.GetKeys(snapshotIndexName)
	require.Len(t, keys, 1, "unexpected number of keys")
	require.Equal(t, snapIDKey, keys[0])

	idx.RemoveFromIndex(snapIDKey, snapshotIndexName)

	keys = idx.GetKeys(snapshotIndexName)
	require.Empty(t, keys)
}
