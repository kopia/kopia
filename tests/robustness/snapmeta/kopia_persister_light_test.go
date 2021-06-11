// +build darwin,amd64 linux,amd64

package snapmeta

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
)

var (
	key = "mykey"
	val = []byte("myval")
)

func TestStoreLoadDelete(t *testing.T) {
	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	defer os.RemoveAll(repoPath)

	kpl := initKPL(t, repoPath)
	defer kpl.Cleanup()

	kpl.testStoreLoad(t, key, val)
	kpl.testDelete(t, key)
}

func TestConcurrency(t *testing.T) {
	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	defer os.RemoveAll(repoPath)

	kpl := initKPL(t, repoPath)
	defer kpl.Cleanup()

	keys := []string{"key1", "key2", "key3"}
	vals := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}

	t.Run("storeLoad", func(t *testing.T) {
		for i := 0; i < 9; i++ {
			j := i
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				kpl.testStoreLoad(t, keys[j%3], vals[j%3])
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		for i := 0; i < 9; i++ {
			j := i
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Parallel()
				kpl.testDelete(t, keys[j%3])
			})
		}
	})
}

// Store and test that subsequent Load succeeds.
func (kpl *KopiaPersisterLight) testStoreLoad(t *testing.T, key string, val []byte) { //nolint:thelper
	err := kpl.Store(key, val)
	assertNoError(t, err)

	valOut, err := kpl.Load(key)
	assertNoError(t, err)

	if !bytes.Equal(valOut, val) {
		t.Fatal("loaded value does not equal stored value")
	}
}

// Delete and test that subsequent Load fails.
func (kpl *KopiaPersisterLight) testDelete(t *testing.T, key string) { //nolint:thelper
	kpl.Delete(key)

	_, err := kpl.Load(key)
	log.Println("err:", err)

	if err == nil {
		t.Fatal("snapshot was not deleted properly")
	}
}

func TestPersistence(t *testing.T) {
	repoPath, err := os.MkdirTemp("", "kopia-test-repo-")
	assertNoError(t, err)

	kpl := initKPL(t, repoPath)

	// Store and cleanup kpl
	err = kpl.Store(key, val)
	assertNoError(t, err)

	kpl.Cleanup()

	// Re-initialize and Load
	kpl = initKPL(t, repoPath)
	valOut, err := kpl.Load(key)
	assertNoError(t, err)

	if !bytes.Equal(valOut, val) {
		t.Fatal("loaded value does not equal stored value")
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
