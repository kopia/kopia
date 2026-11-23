package upload

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// A symlink whose target changes to a different path of the SAME byte length,
// with unchanged mtime/mode/owner, must not be treated as unchanged by the
// incremental cache-reuse check: metadataEquals compares only mtime/mode/owner
// and Size(), and for a symlink Size() is the target's byte length, never the
// target itself. Reusing the cached entry would store the OLD target -> silent
// data corruption on restore.
func TestSymlinkTargetChangeSameLengthNotStale(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)
	t.Cleanup(th.cleanup)

	u := NewUploader(th.repo)
	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	// First snapshot: link -> "aaaa".
	root1 := mockfs.NewDirectory()
	root1.AddSymlink("link", "aaaa", defaultPermissions)

	man1, err := u.Upload(ctx, root1, policyTree, snapshot.SourceInfo{})
	require.NoError(t, err)

	// Second snapshot: same name/mtime/mode/owner, target changed to "bbbb"
	// (same 4-byte length as "aaaa").
	root2 := mockfs.NewDirectory()
	root2.AddSymlink("link", "bbbb", defaultPermissions)

	man2, err := u.Upload(ctx, root2, policyTree, snapshot.SourceInfo{}, man1)
	require.NoError(t, err)

	require.Equal(t, "bbbb", readSymlinkTarget(ctx, t, th, man2, "link"),
		"second snapshot must store the new symlink target, not the stale cached one")
}

// The fix must stay surgical: an unchanged symlink is still served from the
// incremental cache (no re-hash), so identical re-uploads report zero hashed
// files.
func TestSymlinkUnchangedStillCached(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)
	t.Cleanup(th.cleanup)

	u := NewUploader(th.repo)
	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	root := mockfs.NewDirectory()
	root.AddSymlink("link", "some/target", defaultPermissions)

	man1, err := u.Upload(ctx, root, policyTree, snapshot.SourceInfo{})
	require.NoError(t, err)

	man2, err := u.Upload(ctx, root, policyTree, snapshot.SourceInfo{}, man1)
	require.NoError(t, err)

	require.Equal(t, int32(0), atomic.LoadInt32(&man2.Stats.TotalFileCount),
		"unchanged symlink must be reused from cache, not re-hashed")
	require.Equal(t, "some/target", readSymlinkTarget(ctx, t, th, man2, "link"))
}

func readSymlinkTarget(ctx context.Context, t *testing.T, th *uploadTestHarness, man *snapshot.Manifest, name string) string {
	t.Helper()

	dir := testutil.EnsureType[fs.Directory](t, snapshotfs.EntryFromDirEntry(th.repo, man.RootEntry))

	child, err := dir.Child(ctx, name)
	require.NoError(t, err)

	target, err := testutil.EnsureType[fs.Symlink](t, child).Readlink(ctx)
	require.NoError(t, err)

	return target
}
