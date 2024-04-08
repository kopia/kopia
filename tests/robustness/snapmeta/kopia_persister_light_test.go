//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"bytes"
	"context"
	"log"
	"os"
	"strconv"
	"testing"
)

var (
	key = "mykey"
	val = []byte("myval")
)

func TestStoreLoadDelete(t *testing.T) {
	ctx := context.Background()

	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	defer os.RemoveAll(repoPath)

	kpl := initKPL(t, repoPath)
	defer kpl.Cleanup()

	kpl.testStoreLoad(ctx, t, key, val)
	kpl.testDelete(ctx, t, key)
}

func TestConcurrency(t *testing.T) {
	ctx := context.Background()

	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	defer os.RemoveAll(repoPath)

	kpl := initKPL(t, repoPath)
	defer kpl.Cleanup()

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
	assertNoError(t, err)

	valOut, err := kpl.Load(ctx, key)
	assertNoError(t, err)

	if !bytes.Equal(valOut, val) {
		t.Fatal("loaded value does not equal stored value", valOut, val)
	}
}

// Delete and test that subsequent Load fails.
func (kpl *KopiaPersisterLight) testDelete(ctx context.Context, t *testing.T, key string) { //nolint:thelper
	kpl.Delete(ctx, key)

	_, err := kpl.Load(ctx, key)
	log.Println("err:", err)

	if err == nil {
		t.Fatal("snapshot was not deleted properly")
	}
}

func TestPersistence(t *testing.T) {
	ctx := context.Background()

	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	kpl := initKPL(t, repoPath)

	// Persistence directory should be set.
	if persistDir := kpl.GetPersistDir(); persistDir == "" {
		t.Error("could not get persistence directory")
	}

	// These are no-ops and should always succeed.
	err = kpl.LoadMetadata()
	assertNoError(t, err)
	err = kpl.FlushMetadata()
	assertNoError(t, err)

	// Store and cleanup kpl
	err = kpl.Store(ctx, key, val)
	assertNoError(t, err)

	kpl.Cleanup()

	// Re-initialize and Load
	kpl = initKPL(t, repoPath)
	valOut, err := kpl.Load(ctx, key)
	assertNoError(t, err)

	if !bytes.Equal(valOut, val) {
		t.Fatal("loaded value does not equal stored value")
	}

	kpl.Cleanup()
	os.RemoveAll(repoPath)
}

func TestS3Connect(t *testing.T) {
	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	// Test the S3 code path by attempting to connect to a nonexistent bucket.
	t.Setenv(S3BucketNameEnvKey, "does-not-exist")

	kpl, err := NewPersisterLight("")
	assertNoError(t, err)

	if err := kpl.ConnectOrCreateRepo(repoPath); err == nil {
		t.Error("should not be able to connect to nonexistent S3 bucket")
	}

	kpl.Cleanup()
	os.RemoveAll(repoPath)
}

func initKPL(t *testing.T, repoPath string) *KopiaPersisterLight { //nolint:thelper
	os.Unsetenv(S3BucketNameEnvKey)

	kpl, err := NewPersisterLight("")
	assertNoError(t, err)

	err = kpl.ConnectOrCreateRepo(repoPath)
	assertNoError(t, err)

	return kpl
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}
