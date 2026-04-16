//go:build darwin || (linux && amd64)

package snapmeta

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

const key = "mykey"

var val = []byte("myval")

func TestStoreLoadDelete(t *testing.T) {
	repoPath := t.TempDir()
	kpl := initKPL(t, repoPath)

	t.Cleanup(kpl.Cleanup)

	ctx := context.Background()

	kpl.testStoreLoad(ctx, t, key, val)
	kpl.testDelete(ctx, t, key)
}

func TestConcurrency(t *testing.T) {
	ctx := context.Background()

	repoPath := t.TempDir()
	kpl := initKPL(t, repoPath)

	t.Cleanup(kpl.Cleanup)

	keys := []string{"key1", "key2", "key3"}
	vals := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}

	t.Run("storeLoad", func(t *testing.T) {
		for j := range 9 {
			t.Run(strconv.Itoa(j), func(t *testing.T) {
				t.Parallel()
				kpl.testStoreLoad(ctx, t, keys[j%3], vals[j%3])
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		for j := range 9 {
			t.Run(strconv.Itoa(j), func(t *testing.T) {
				t.Parallel()
				kpl.testDelete(ctx, t, keys[j%3])
			})
		}
	})
}

// Store and test that subsequent Load succeeds.
func (kpl *KopiaPersisterLight) testStoreLoad(ctx context.Context, t *testing.T, key string, val []byte) { //nolint:thelper
	err := kpl.Store(ctx, key, val)
	require.NoError(t, err)

	got, err := kpl.Load(ctx, key)
	require.NoError(t, err)

	require.Equal(t, val, got)
}

// Delete and test that subsequent Load fails.
func (kpl *KopiaPersisterLight) testDelete(ctx context.Context, t *testing.T, key string) { //nolint:thelper
	kpl.Delete(ctx, key)

	_, err := kpl.Load(ctx, key)

	require.Error(t, err, "snapshot was not deleted properly")
}

func TestPersistence(t *testing.T) {
	ctx := context.Background()

	repoPath := t.TempDir()

	kpl := initKPL(t, repoPath)

	// Persistence directory should be set.
	persistDir := kpl.GetPersistDir()
	require.NotEmpty(t, persistDir, "could not get persistence directory")

	// These are no-ops and should always succeed.
	err := kpl.LoadMetadata()
	require.NoError(t, err)
	err = kpl.FlushMetadata()
	require.NoError(t, err)

	// Store and cleanup kpl
	err = kpl.Store(ctx, key, val)
	require.NoError(t, err)

	kpl.Cleanup()

	// Re-initialize and Load
	kpl = initKPL(t, repoPath)
	t.Cleanup(kpl.Cleanup)

	got, err := kpl.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val, got, "loaded value is not equal to stored value")
}

func TestS3Connect(t *testing.T) {
	repoPath := t.TempDir()

	// Test the S3 code path by attempting to connect to a nonexistent bucket.
	t.Setenv(S3BucketNameEnvKey, "does-not-exist")

	kpl, err := NewPersisterLight("")
	require.NoError(t, err)

	err = kpl.ConnectOrCreateRepo(repoPath)
	require.Error(t, err, "should not be able to connect to nonexistent S3 bucket")

	kpl.Cleanup()
}

func initKPL(t *testing.T, repoPath string) *KopiaPersisterLight {
	t.Helper()

	os.Unsetenv(S3BucketNameEnvKey)

	kpl, err := NewPersisterLight("")
	require.NoError(t, err)

	err = kpl.ConnectOrCreateRepo(repoPath)
	require.NoError(t, err)

	return kpl
}
