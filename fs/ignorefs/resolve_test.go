package ignorefs

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestNoInfiniteResolveLink(t *testing.T) {
	root := mockfs.NewDirectory()

	root.AddSymlink("a", "./b", 0)
	root.AddSymlink("b", "./c", 0)
	root.AddSymlink("c", "./a", 0)

	ctx := testlogging.Context(t)
	e, err := root.Child(ctx, "b")
	require.NoError(t, err)

	s, ok := e.(fs.Symlink)
	require.True(t, ok)

	f, err := resolveSymlink(ctx, s)

	require.ErrorIs(t, err, errTooManySymlinks)
	require.Nil(t, f)
}
