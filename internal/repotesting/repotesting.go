// Package repotesting contains test utilities for working with repositories.
package repotesting

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/repo/object"

	"github.com/kopia/repo/block"

	"github.com/kopia/repo"
	"github.com/kopia/repo/storage"
	"github.com/kopia/repo/storage/filesystem"
)

const masterPassword = "foobarbazfoobarbaz"

// Environment encapsulates details of a test environment.
type Environment struct {
	Repository *repo.Repository

	configDir  string
	storageDir string
}

// Setup sets up a test environment.
func (e *Environment) Setup(t *testing.T, opts ...func(*repo.NewRepositoryOptions)) *Environment {
	var err error

	ctx := context.Background()

	e.configDir, err = ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	e.storageDir, err = ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	opt := &repo.NewRepositoryOptions{
		BlockFormat: block.FormattingOptions{
			HMACSecret:  []byte{},
			BlockFormat: "UNENCRYPTED_HMAC_SHA256",
		},
		ObjectFormat: object.Format{
			Splitter:     "FIXED",
			MaxBlockSize: 400,
		},
		MetadataEncryptionAlgorithm: "NONE",
	}

	for _, mod := range opts {
		mod(opt)
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

	connOpts := repo.ConnectOptions{
		//TraceStorage: log.Printf,
	}

	if err = repo.Connect(ctx, e.configFile(), st, masterPassword, connOpts); err != nil {
		t.Fatalf("can't connect: %v", err)
	}

	e.Repository, err = repo.Open(ctx, e.configFile(), masterPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("can't open: %v", err)
	}

	return e
}

// Close closes testing environment
func (e *Environment) Close(t *testing.T) {
	if err := e.Repository.Close(context.Background()); err != nil {
		t.Fatalf("unable to close: %v", err)
	}

	if err := os.RemoveAll(e.configDir); err != nil {
		t.Errorf("error removing config directory: %v", err)
	}
	if err := os.RemoveAll(e.storageDir); err != nil {
		t.Errorf("error removing storage directory: %v", err)
	}
}

func (e *Environment) configFile() string {
	return filepath.Join(e.configDir, "kopia.config")
}

// MustReopen closes and reopens the repository.
func (e *Environment) MustReopen(t *testing.T) {
	err := e.Repository.Close(context.Background())
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	e.Repository, err = repo.Open(context.Background(), e.configFile(), masterPassword, &repo.Options{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// VerifyStorageBlockCount verifies that the underlying storage contains the specified number of blocks.
func (e *Environment) VerifyStorageBlockCount(t *testing.T, want int) {
	var got int

	_ = e.Repository.Storage.ListBlocks(context.Background(), "", func(_ storage.BlockMetadata) error {
		got++
		return nil
	})

	if got != want {
		t.Errorf("got unexpected number of storage blocks: %v, wanted %v", got, want)
	}
}
