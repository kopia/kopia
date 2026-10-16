//go:build !windows

package localfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
)

func TestHardLinkInfo(t *testing.T) {
	ctx := testlogging.Context(t)
	tmp := testutil.TempDirectory(t)

	target := filepath.Join(tmp, "target")
	link1 := filepath.Join(tmp, "link1")
	link2 := filepath.Join(tmp, "link2")
	other := filepath.Join(tmp, "other")

	require.NoError(t, os.WriteFile(target, []byte{1, 2, 3}, 0o644))
	require.NoError(t, os.Link(target, link1))
	require.NoError(t, os.Link(target, link2))
	require.NoError(t, os.WriteFile(other, []byte{1, 2, 3}, 0o644))

	targetInfo := hardLinkInfoOf(t, target)
	require.NotZero(t, targetInfo.UniqID)
	require.Equal(t, uint64(3), targetInfo.NLink)

	require.Equal(t, targetInfo, hardLinkInfoOf(t, link1))
	require.Equal(t, targetInfo, hardLinkInfoOf(t, link2))

	otherInfo := hardLinkInfoOf(t, other)
	require.NotEqual(t, targetInfo.UniqID, otherInfo.UniqID)
	require.Equal(t, uint64(1), otherInfo.NLink)

	// entries produced by directory iteration must carry the same info as direct lookup.
	dir, err := Directory(tmp)
	require.NoError(t, err)

	seen := map[string]fs.HardLinkInfo{}

	require.NoError(t, fs.IterateEntries(ctx, dir, func(_ context.Context, e fs.Entry) error {
		seen[e.Name()] = e.HardLinkInfo()
		return nil
	}))

	require.Equal(t, targetInfo, seen["target"])
	require.Equal(t, targetInfo, seen["link1"])
	require.Equal(t, targetInfo, seen["link2"])
	require.Equal(t, otherInfo, seen["other"])
}

func TestHardLinkInfoDirectory(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	require.NoError(t, os.Mkdir(filepath.Join(tmp, "sub"), 0o755))

	info := hardLinkInfoOf(t, filepath.Join(tmp, "sub"))
	require.NotZero(t, info.UniqID)
	require.NotZero(t, info.NLink)
}

func hardLinkInfoOf(t *testing.T, path string) fs.HardLinkInfo {
	t.Helper()

	e, err := NewEntry(path)
	require.NoError(t, err)

	return e.HardLinkInfo()
}
