//go:build !release && rcbuild

package repotesting

// Extension to repotesting.go providing a test Environment backed by "real" (user-supplied) storage

import (
	"context"
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/stretchr/testify/require"
)

// NewEnvironmentWithStorage creates a new repository testing environment backed by user-supplied storage and ensures its cleanup at the end of the test.
func NewEnvironmentWithStorage(tb testing.TB, storage *blob.Storage, version format.Version, opts ...Options) (ctx context.Context, env *Environment) {
	tb.Helper()

	ctx = testlogging.Context(tb)
	env = &Environment{}

	env.setupWithStorage(tb, storage, version, opts...)

	tb.Cleanup(func() {
		env.Close(ctx, tb)
	})

	return ctx, env
}

// setup sets up a test environment backed by user-provided storage instance
func (e *Environment) setupWithStorage(tb testing.TB, storage *blob.Storage, version format.Version, opts ...Options) *Environment {
	tb.Helper()

	ctx := testlogging.Context(tb)
	e.configDir = testutil.TempDirectory(tb)
	openOpt := &repo.Options{}

	opt := &repo.NewRepositoryOptions{
		BlockFormat: format.ContentFormat{
			MutableParameters: format.MutableParameters{
				Version: version,
			},
			HMACSecret:           []byte("a-repository-testing-hmac-secret"),
			Hash:                 "HMAC-SHA256",
			Encryption:           encryption.DefaultAlgorithm,
			EnablePasswordChange: true,
		},
		ObjectFormat: format.ObjectFormat{
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

	st := NewReconnectableStorage(tb, *storage)
	e.st = st

	if e.Password == "" {
		e.Password = DefaultPasswordForTesting
	}

	if err := repo.Initialize(ctx, st, opt, e.Password); err != nil {
		tb.Fatalf("err: %v", err)
	}

	if err := repo.Connect(ctx, e.ConfigFile(), st, e.Password, nil); err != nil {
		tb.Fatalf("can't connect: %v", err)
	}

	e.connected = true

	// ensure context passed to Open() is not used beyond its scope.
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	rep, err := repo.Open(ctx2, e.ConfigFile(), e.Password, openOpt)

	require.NoError(tb, err)

	e.Repository = rep

	_, e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		e.RepositoryWriter.Close(ctx)
		rep.Close(ctx)
	})

	return e
}
