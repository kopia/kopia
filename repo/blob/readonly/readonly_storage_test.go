package readonly_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/sharded"
)

func TestIsReadOnly(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	path := testutil.TempDirectory(t)

	r, err := filesystem.New(ctx, &filesystem.Options{
		Path:    path,
		Options: sharded.Options{},
	}, true)

	require.NoError(t, err)
	require.NotNil(t, r)

	require.False(t, readonly.IsReadOnly(r))
	require.True(t, readonly.IsReadOnly(readonly.NewWrapper(r)))
}
