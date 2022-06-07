package snapshotfs_test

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

type fakeProgress struct {
	t                   *testing.T
	expectedFiles       int32
	expectedDirectories int32
	expectedErrors      int32
}

func (p *fakeProgress) Processing(context.Context, string) {}

func (p *fakeProgress) Error(context.Context, string, error, bool) {}

// +checklocksignore.
func (p *fakeProgress) Stats(
	ctx context.Context,
	s *snapshot.Stats,
	includedFiles, excludedFiles snapshotfs.SampleBuckets,
	excludedDirs []string,
	final bool,
) {
	if !final {
		return
	}

	if got := s.ErrorCount; got != p.expectedErrors {
		p.t.Errorf("unexpected errors encountered: (actual) %v != %v (expected)", got, p.expectedErrors)
	}

	if got := s.TotalFileCount; got != p.expectedFiles {
		p.t.Errorf("unexpected files counted: (actual) %v != %v (expected)", got, p.expectedFiles)
	}

	if got := s.TotalDirectoryCount; got != p.expectedDirectories {
		p.t.Errorf("unexpected directory count: (actual) %v != %v (expected)", got, p.expectedDirectories)
	}
}

func TestEstimate_SkipsStreamingDirectory(t *testing.T) {
	f := mockfs.NewFile("f1", []byte{1, 2, 3}, 0o777)

	rootDir := virtualfs.NewStaticDirectory("root", []fs.Entry{
		virtualfs.NewStreamingDirectory(
			"a-dir",
			func(ctx context.Context, callback func(context.Context, fs.Entry) error) error {
				return callback(ctx, f)
			},
		),
	})

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
	p := &fakeProgress{
		t:                   t,
		expectedFiles:       0,
		expectedDirectories: 2,
		expectedErrors:      0,
	}

	err := snapshotfs.Estimate(context.TODO(), nil, rootDir, policyTree, p, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
