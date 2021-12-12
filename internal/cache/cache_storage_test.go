package cache_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

func TestNewStorageOrNil(t *testing.T) {
	ctx := testlogging.Context(t)

	// empty cache dir
	st, err := cache.NewStorageOrNil(ctx, "", 1000, "subdir")
	require.NoError(t, err)
	require.Nil(t, st)

	// zero size
	st, err = cache.NewStorageOrNil(ctx, testutil.TempDirectory(t), 0, "subdir")
	require.NoError(t, err)
	require.Nil(t, st)

	_, err = cache.NewStorageOrNil(ctx, "relative/path/to/cache/dir", 1000, "subdir")
	require.Error(t, err)
}
