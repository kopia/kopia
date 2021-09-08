// Package repotesting contains test utilities for working with repositories.
package repotesting

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/object"
)

const defaultPassword = "foobarbazfoobarbaz"

// Environment encapsulates details of a test environment.
type Environment struct {
	Repository       repo.Repository
	RepositoryWriter repo.DirectRepositoryWriter

	Password string

	configDir string
	st        blob.Storage
	connected bool
}

// Options used during Environment Setup.
type Options struct {
	NewRepositoryOptions func(*repo.NewRepositoryOptions)
	OpenOptions          func(*repo.Options)
}

// setup sets up a test environment.
func (e *Environment) setup(t *testing.T, version content.FormatVersion, opts ...Options) *Environment {
	t.Helper()

	ctx := testlogging.Context(t)
	e.configDir = testutil.TempDirectory(t)
	openOpt := &repo.Options{}

	opt := &repo.NewRepositoryOptions{
		BlockFormat: content.FormattingOptions{
			Version:              version,
			HMACSecret:           []byte{},
			Hash:                 "HMAC-SHA256",
			Encryption:           encryption.DefaultAlgorithm,
			EnablePasswordChange: true,
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

	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, openOpt.TimeNowFunc)
	st = newReconnectableStorage(t, st)
	e.st = st

	if e.Password == "" {
		e.Password = defaultPassword
	}

	if err := repo.Initialize(ctx, st, opt, e.Password); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := repo.Connect(ctx, e.ConfigFile(), st, e.Password, nil); err != nil {
		t.Fatalf("can't connect: %v", err)
	}

	e.connected = true

	rep, err := repo.Open(ctx, e.ConfigFile(), e.Password, openOpt)
	if err != nil {
		t.Fatalf("can't open: %v", err)
	}

	e.Repository = rep

	_, e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { rep.Close(ctx) })

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

	rep, err := repo.Open(ctx, e.ConfigFile(), e.Password, repoOptions(openOpts))
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	t.Cleanup(func() { rep.Close(ctx) })

	_, e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// MustOpenAnother opens another repository backend by the same storage.
func (e *Environment) MustOpenAnother(t *testing.T) repo.RepositoryWriter {
	t.Helper()

	ctx := testlogging.Context(t)

	rep2, err := repo.Open(ctx, e.ConfigFile(), e.Password, &repo.Options{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	t.Cleanup(func() {
		rep2.Close(ctx)
	})

	_, w, err := rep2.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
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

	config := filepath.Join(testutil.TempDirectory(t), "kopia.config")
	connOpts := &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory: testutil.TempDirectory(t),
		},
	}

	if err := repo.Connect(ctx, config, e.st, e.Password, connOpts); err != nil {
		t.Fatal("can't connect:", err)
	}

	rep, err := repo.Open(ctx, e.ConfigFile(), e.Password, repoOptions(openOpts))
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

// FormatNotImportant chooses arbitrary format version where it's not important to the test.
const FormatNotImportant = content.FormatVersion2

// NewEnvironment creates a new repository testing environment and ensures its cleanup at the end of the test.
func NewEnvironment(t *testing.T, version content.FormatVersion, opts ...Options) (context.Context, *Environment) {
	t.Helper()

	ctx := testlogging.Context(t)

	var env Environment

	env.setup(t, version, opts...)

	t.Cleanup(func() {
		env.Close(ctx, t)
	})

	return ctx, &env
}
