// Package repotesting contains test utilities for working with repositories.
package repotesting

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/object"
)

const masterPassword = "foobarbazfoobarbaz"

// Environment encapsulates details of a test environment.
type Environment struct {
	Repository       repo.Repository
	RepositoryWriter repo.DirectRepositoryWriter

	configDir  string
	storageDir string
	connected  bool
}

// Options used during Environment Setup.
type Options struct {
	NewRepositoryOptions func(*repo.NewRepositoryOptions)
	OpenOptions          func(*repo.Options)
}

// Setup sets up a test environment.
func (e *Environment) Setup(t *testing.T, opts ...Options) *Environment {
	t.Helper()

	ctx := testlogging.Context(t)
	e.configDir = testutil.TempDirectory(t)
	e.storageDir = testutil.TempDirectory(t)
	openOpt := &repo.Options{}

	opt := &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			HMACSecret: []byte{},
			Hash:       "HMAC-SHA256",
			Encryption: encryption.DefaultAlgorithm,
		},
		ObjectFormat: object.Format{
			Splitter: "FIXED-1M",
		},
	}

	for _, mod := range opts {
		if mod.NewRepositoryOptions != nil {
			mod.NewRepositoryOptions(opt)
		}

		if mod.OpenOptions != nil {
			mod.OpenOptions(openOpt)
		}
	}

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: e.storageDir,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err = repo.Initialize(ctx, st, opt, masterPassword); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err = repo.Connect(ctx, e.ConfigFile(), st, masterPassword, nil); err != nil {
		t.Fatalf("can't connect: %v", err)
	}

	e.connected = true

	rep, err := repo.Open(ctx, e.ConfigFile(), masterPassword, openOpt)
	if err != nil {
		t.Fatalf("can't open: %v", err)
	}

	e.Repository = rep

	e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		t.Fatal(err)
	}

	return e
}

// Close closes testing environment.
func (e *Environment) Close(ctx context.Context, t *testing.T) {
	t.Helper()

	if err := e.RepositoryWriter.Close(ctx); err != nil {
		t.Fatalf("unable to close: %v", err)
	}

	if e.connected {
		if err := repo.Disconnect(ctx, e.ConfigFile()); err != nil {
			t.Errorf("error disconnecting: %v", err)
		}
	}

	if err := os.Remove(e.configDir); err != nil {
		// should be empty, assuming Disconnect was successful
		t.Errorf("error removing config directory: %v", err)
	}
}

// ConfigFile returns the name of the config file.
func (e *Environment) ConfigFile() string {
	return filepath.Join(e.configDir, "kopia.config")
}

// MustReopen closes and reopens the repository.
func (e *Environment) MustReopen(t *testing.T, openOpts ...func(*repo.Options)) {
	t.Helper()

	ctx := testlogging.Context(t)

	err := e.RepositoryWriter.Close(ctx)
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	rep, err := repo.Open(ctx, e.ConfigFile(), masterPassword, repoOptions(openOpts))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// MustOpenAnother opens another repository backend by the same storage.
func (e *Environment) MustOpenAnother(t *testing.T) repo.RepositoryWriter {
	t.Helper()

	ctx := testlogging.Context(t)

	rep2, err := repo.Open(ctx, e.ConfigFile(), masterPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	t.Cleanup(func() {
		rep2.Close(ctx)
	})

	w, err := rep2.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		t.Fatal(err)
	}

	return w
}

// MustConnectOpenAnother opens another repository backend by the same storage,
// with independent config and cache options.
func (e *Environment) MustConnectOpenAnother(t *testing.T, openOpts ...func(*repo.Options)) repo.Repository {
	t.Helper()

	ctx := testlogging.Context(t)

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path: e.storageDir,
	})
	if err != nil {
		t.Fatal("err:", err)
	}

	config := filepath.Join(testutil.TempDirectory(t), "kopia.config")
	connOpts := &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory: testutil.TempDirectory(t),
		},
	}

	if err = repo.Connect(ctx, config, st, masterPassword, connOpts); err != nil {
		t.Fatal("can't connect:", err)
	}

	rep, err := repo.Open(ctx, e.ConfigFile(), masterPassword, repoOptions(openOpts))
	if err != nil {
		t.Fatal("can't open:", err)
	}

	return rep
}

// VerifyBlobCount verifies that the underlying storage contains the specified number of blobs.
func (e *Environment) VerifyBlobCount(t *testing.T, want int) {
	t.Helper()

	var got int

	_ = e.RepositoryWriter.BlobReader().ListBlobs(testlogging.Context(t), "", func(_ blob.Metadata) error {
		got++
		return nil
	})

	if got != want {
		t.Errorf("got unexpected number of BLOBs: %v, wanted %v", got, want)
	}
}

func repoOptions(openOpts []func(*repo.Options)) *repo.Options {
	openOpt := &repo.Options{}

	for _, mod := range openOpts {
		if mod != nil {
			mod(openOpt)
		}
	}

	return openOpt
}
