// Package repotesting contains test utilities for working with repositories.
package repotesting

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/snapshot"
)

// DefaultPasswordForTesting is the default password to use for all testing repositories.
const DefaultPasswordForTesting = "foobarbazfoobarbaz"

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

// RepositoryMetrics returns metrics.Registry associated with a repository.
func (e *Environment) RepositoryMetrics() *metrics.Registry {
	return e.Repository.(interface {
		Metrics() *metrics.Registry
	}).Metrics()
}

// RootStorage returns the base storage map that implements the base in-memory
// map at the base of all storage wrappers on top.
func (e *Environment) RootStorage() blob.Storage {
	return e.st.(reconnectableStorage).Storage
}

// setup sets up a test environment.
func (e *Environment) setup(tb testing.TB, version format.Version, opts ...Options) *Environment {
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

	var st blob.Storage
	if opt.RetentionPeriod == 0 || opt.RetentionMode == "" {
		st = blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, openOpt.TimeNowFunc)
	} else {
		// use versioned mock storage when retention settings are specified
		st = blobtesting.NewVersionedMapStorage(openOpt.TimeNowFunc)
	}

	st = NewReconnectableStorage(tb, st)
	e.st = st

	if e.Password == "" {
		e.Password = DefaultPasswordForTesting
	}

	err := repo.Initialize(ctx, st, opt, e.Password)
	require.NoError(tb, err)

	err = repo.Connect(ctx, e.ConfigFile(), st, e.Password, nil)
	require.NoError(tb, err, "can't connect")

	e.connected = true

	// ensure context passed to Open() is not used beyond its scope.
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	rep, err := repo.Open(ctx2, e.ConfigFile(), e.Password, openOpt)
	require.NoError(tb, err)

	e.Repository = rep

	_, e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	require.NoError(tb, err)

	tb.Cleanup(func() {
		e.RepositoryWriter.Close(ctx)
		rep.Close(ctx)
	})

	return e
}

// Close closes testing environment.
func (e *Environment) Close(ctx context.Context, tb testing.TB) {
	tb.Helper()

	err := e.RepositoryWriter.Close(ctx)
	require.NoError(tb, err, "unable to close")

	if e.connected {
		err := repo.Disconnect(ctx, e.ConfigFile())
		require.NoError(tb, err, "error disconnecting")
	}

	err = os.Remove(e.configDir)
	// should be empty, assuming Disconnect was successful
	require.NoError(tb, err, "error removing config directory")
}

// ConfigFile returns the name of the config file.
func (e *Environment) ConfigFile() string {
	return filepath.Join(e.configDir, "kopia.config")
}

// MustReopen closes and reopens the repository.
func (e *Environment) MustReopen(tb testing.TB, openOpts ...func(*repo.Options)) {
	tb.Helper()

	ctx := testlogging.Context(tb)

	err := e.RepositoryWriter.Close(ctx)
	require.NoError(tb, err, "close error")

	// ensure context passed to Open() is not used for cancellation signal.
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	rep, err := repo.Open(ctx2, e.ConfigFile(), e.Password, repoOptions(openOpts))
	require.NoError(tb, err)

	tb.Cleanup(func() { rep.Close(ctx) })

	_, e.RepositoryWriter, err = rep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	require.NoError(tb, err)
}

// MustOpenAnother opens another repository backed by the same storage location.
func (e *Environment) MustOpenAnother(tb testing.TB, openOpts ...func(*repo.Options)) repo.RepositoryWriter {
	tb.Helper()

	ctx := testlogging.Context(tb)

	rep2, err := repo.Open(ctx, e.ConfigFile(), e.Password, repoOptions(openOpts))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		rep2.Close(ctx)
	})

	_, w, err := rep2.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	require.NoError(tb, err)

	return w
}

// MustConnectOpenAnother opens another repository backed by the same storage,
// with independent config and cache options.
func (e *Environment) MustConnectOpenAnother(tb testing.TB, openOpts ...func(*repo.Options)) repo.Repository {
	tb.Helper()

	ctx := testlogging.Context(tb)

	config := filepath.Join(testutil.TempDirectory(tb), "kopia.config")
	connOpts := &repo.ConnectOptions{
		CachingOptions: content.CachingOptions{
			CacheDirectory: testutil.TempDirectory(tb),
		},
	}

	err := repo.Connect(ctx, config, e.st, e.Password, connOpts)
	require.NoError(tb, err, "can't connect")

	rep, err := repo.Open(ctx, e.ConfigFile(), e.Password, repoOptions(openOpts))
	require.NoError(tb, err, "can't open")

	return rep
}

// VerifyBlobCount verifies that the underlying storage contains the specified number of blobs.
func (e *Environment) VerifyBlobCount(tb testing.TB, want int) {
	tb.Helper()

	var got int

	_ = e.RepositoryWriter.BlobReader().ListBlobs(testlogging.Context(tb), "", func(_ blob.Metadata) error {
		got++
		return nil
	})

	require.Equal(tb, want, got, "got unexpected number of BLOBs")
}

// LocalPathSourceInfo is a convenience method that returns SourceInfo for the local user and path.
func (e *Environment) LocalPathSourceInfo(path string) snapshot.SourceInfo {
	return snapshot.SourceInfo{
		UserName: e.Repository.ClientOptions().Username,
		Host:     e.Repository.ClientOptions().Hostname,
		Path:     path,
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
const FormatNotImportant = format.FormatVersion3

// NewEnvironment creates a new repository testing environment and ensures its cleanup at the end of the test.
func NewEnvironment(tb testing.TB, version format.Version, opts ...Options) (context.Context, *Environment) {
	tb.Helper()

	ctx := testlogging.Context(tb)

	var env Environment

	env.setup(tb, version, opts...)

	tb.Cleanup(func() {
		env.Close(ctx, tb)
	})

	return ctx, &env
}
