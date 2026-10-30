package repotesting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/upload"
)

func TestTimeFuncWiring(t *testing.T) {
	ctx, env := NewEnvironment(t, FormatNotImportant)

	ft := faketime.NewTimeAdvance(time.Date(2018, time.February, 6, 0, 0, 0, 0, time.UTC))

	// Re open with injected time
	rep, err := repo.Open(ctx, env.RepositoryWriter.ConfigFilename(), env.Password, &repo.Options{TimeNowFunc: ft.NowFunc()})
	require.NoError(t, err, "failed to open test repository")

	r0 := testutil.EnsureType[repo.DirectRepository](t, rep)

	_, env.RepositoryWriter, err = r0.NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	require.NoError(t, err)

	// verify wiring for the repo layer
	got, want := env.RepositoryWriter.Time(), ft.NowFunc()()
	require.WithinDuration(t, want, got, 0, "times do not match")

	want, got = ft.Advance(10*time.Minute), env.RepositoryWriter.Time()
	require.WithinDuration(t, want, got, 0, "times do not match")

	// verify wiring for the content layer
	nt := ft.Advance(20 * time.Second)

	cid, err := env.RepositoryWriter.ContentManager().WriteContent(ctx, gather.FromSlice([]byte("foo")), "", content.NoCompression)
	require.NoError(t, err, "failed to write content")

	info, err := env.RepositoryWriter.ContentInfo(ctx, cid)

	require.NoErrorf(t, err, "failed to get content info for %s", cid)
	require.WithinDuration(t, nt, info.Timestamp(), 0, "content time does not match")

	// verify wiring for the manifest layer
	nt = ft.Advance(3 * time.Minute)

	labels := map[string]string{"l1": "v1", "l2": "v2", "type": "my-manifest"}

	mid, err := env.RepositoryWriter.PutManifest(ctx, labels, "manifest content")
	require.NoError(t, err, "failed to put manifest")

	meta, err := env.RepositoryWriter.GetManifest(ctx, mid, nil)

	require.NoError(t, err, "failed to get manifest metadata")
	require.WithinDuration(t, nt, meta.ModTime, 0, "manifest modification time does not match")

	const defaultPermissions = 0o777

	// verify wiring for the snapshot layer
	sourceDir := mockfs.NewDirectory()
	sourceDir.AddFile("f1", []byte{1, 2, 3}, defaultPermissions)

	nt = ft.Advance(1 * time.Hour)
	u := upload.NewUploader(env.RepositoryWriter)
	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	s1, err := u.Upload(ctx, sourceDir, policyTree, snapshot.SourceInfo{})

	require.NoError(t, err, "failed to create snapshot")
	require.WithinDuration(t, nt, s1.StartTime.ToTime(), 0, "snapshot time does not match")
}
