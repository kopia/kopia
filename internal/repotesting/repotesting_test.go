package repotesting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestTimeFuncWiring(t *testing.T) {
	ctx, env := NewEnvironment(t, FormatNotImportant)

	ft := faketime.NewTimeAdvance(time.Date(2018, time.February, 6, 0, 0, 0, 0, time.UTC))

	// Re open with injected time
	rep, err := repo.Open(ctx, env.RepositoryWriter.ConfigFilename(), env.Password, &repo.Options{TimeNowFunc: ft.NowFunc()})
	if err != nil {
		t.Fatal("Failed to open repo:", err)
	}

	r0 := rep.(repo.DirectRepository)

	_, env.RepositoryWriter, err = r0.NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	require.NoError(t, err)

	// verify wiring for the repo layer
	if got, want := env.RepositoryWriter.Time(), ft.NowFunc()(); !got.Equal(want) {
		t.Errorf("times don't match, got %v, want %v", got, want)
	}

	if want, got := ft.Advance(10*time.Minute), env.RepositoryWriter.Time(); !got.Equal(want) {
		t.Errorf("times don't match, got %v, want %v", got, want)
	}

	// verify wiring for the content layer
	nt := ft.Advance(20 * time.Second)

	cid, err := env.RepositoryWriter.ContentManager().WriteContent(ctx, gather.FromSlice([]byte("foo")), "", content.NoCompression)
	if err != nil {
		t.Fatal("failed to write content:", err)
	}

	info, err := env.RepositoryWriter.ContentInfo(ctx, cid)
	if err != nil {
		t.Fatal("failed to get content info for", cid, err)
	}

	if got, want := info.Timestamp(), nt; !got.Equal(want) {
		t.Errorf("content time does not match, got %v, want %v", got, want)
	}

	// verify wiring for the manifest layer
	nt = ft.Advance(3 * time.Minute)

	labels := map[string]string{"l1": "v1", "l2": "v2", "type": "my-manifest"}

	mid, err := env.RepositoryWriter.PutManifest(ctx, labels, "manifest content")
	if err != nil {
		t.Fatal("failed to put manifest:", err)
	}

	meta, err := env.RepositoryWriter.GetManifest(ctx, mid, nil)
	if err != nil {
		t.Fatal("failed to get manifest metadata:", err)
	}

	if got, want := meta.ModTime, nt; !got.Equal(want) {
		t.Errorf("manifest time does not match, got %v, want %v", got, want)
	}

	const defaultPermissions = 0o777

	// verify wiring for the snapshot layer
	sourceDir := mockfs.NewDirectory()
	sourceDir.AddFile("f1", []byte{1, 2, 3}, defaultPermissions)

	nt = ft.Advance(1 * time.Hour)
	u := snapshotfs.NewUploader(env.RepositoryWriter)
	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	s1, err := u.Upload(ctx, sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Fatal("failed to create snapshot:", err)
	}

	if got, want := nt, s1.StartTime.ToTime(); !got.Equal(want) {
		t.Fatalf("snapshot time does not match, got: %v, want: %v", got, want)
	}
}
