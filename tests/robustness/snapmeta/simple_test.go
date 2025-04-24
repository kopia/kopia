//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/robustness"
)

func TestSimpleBasic(t *testing.T) {
	ctx := context.Background()

	simple := NewSimple()

	gotData, err := simple.Load(ctx, "non-existent-key")
	require.ErrorIs(t, err, robustness.ErrKeyNotFound, "Did not get expected error")
	require.Nil(t, gotData, "Expecting nil data return from a key that does not exist")

	storeKey := "key-to-store"
	data := []byte("some stored data")
	simple.Store(ctx, storeKey, data)

	gotData, err = simple.Load(ctx, storeKey)
	require.NoError(t, err, "Error getting key")
	require.Equal(t, data, gotData, "Did not get the correct data")

	simple.Delete(ctx, storeKey)

	gotData, err = simple.Load(ctx, storeKey)
	require.ErrorIs(t, err, robustness.ErrKeyNotFound, "Did not get expected error")
	require.Nil(t, gotData, "Expecting nil data return from a deleted key")
}
