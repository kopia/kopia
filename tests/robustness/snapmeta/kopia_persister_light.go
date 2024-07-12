//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/tools/kopiaclient"
)

// KopiaPersisterLight is a wrapper for KopiaClient that satisfies the Persister
// interface.
type KopiaPersisterLight struct {
	kc            *kopiaclient.KopiaClient
	keysInProcess map[string]bool
	c             *sync.Cond
	baseDir       string
}

var _ robustness.Persister = (*KopiaPersisterLight)(nil)

// NewPersisterLight returns a new KopiaPersisterLight.
func NewPersisterLight(baseDir string) (*KopiaPersisterLight, error) {
	persistenceDir, err := os.MkdirTemp(baseDir, "kopia-persistence-root-")
	if err != nil {
		return nil, err
	}

	return &KopiaPersisterLight{
		kc:            kopiaclient.NewKopiaClient(persistenceDir),
		keysInProcess: map[string]bool{},
		c:             sync.NewCond(&sync.Mutex{}),
		baseDir:       persistenceDir,
	}, nil
}

// ConnectOrCreateRepo creates a new Kopia repo or connects to an existing one if possible.
func (kpl *KopiaPersisterLight) ConnectOrCreateRepo(repoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	return kpl.kc.CreateOrConnectRepo(context.Background(), repoPath, bucketName)
}

// SetCacheLimits sets to an existing one if possible.
func (kpl *KopiaPersisterLight) SetCacheLimits(repoPath string, cacheOpts *content.CachingOptions) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	err := kpl.kc.SetCacheLimits(context.Background(), repoPath, bucketName, cacheOpts)
	if err != nil {
		return err
	}

	return nil
}

// Store pushes the key value pair to the Kopia repository.
func (kpl *KopiaPersisterLight) Store(ctx context.Context, key string, val []byte) error {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("pushing metadata for", key)

	return kpl.kc.SnapshotCreate(ctx, key, val)
}

// Load pulls the key value pair from the Kopia repo and returns the value.
func (kpl *KopiaPersisterLight) Load(ctx context.Context, key string) ([]byte, error) {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("pulling metadata for", key)

	return kpl.kc.SnapshotRestore(ctx, key)
}

// Delete deletes all snapshots associated with the given key.
func (kpl *KopiaPersisterLight) Delete(ctx context.Context, key string) error {
	kpl.waitFor(key)
	defer kpl.doneWith(key)

	log.Println("deleting metadata for", key)

	return kpl.kc.SnapshotDelete(ctx, key)
}

// LoadMetadata is a no-op. It is included to satisfy the Persister interface.
func (kpl *KopiaPersisterLight) LoadMetadata() error {
	return nil
}

// FlushMetadata is a no-op. It is included to satisfy the Persister interface.
func (kpl *KopiaPersisterLight) FlushMetadata() error {
	return nil
}

// GetPersistDir returns the persistence directory.
func (kpl *KopiaPersisterLight) GetPersistDir() string {
	return kpl.baseDir
}

// Cleanup removes the persistence directory and closes the Kopia repo.
func (kpl *KopiaPersisterLight) Cleanup() {
	if err := os.RemoveAll(kpl.baseDir); err != nil {
		log.Println("cannot remove persistence dir")
	}
}

func (kpl *KopiaPersisterLight) waitFor(key string) {
	kpl.c.L.Lock()
	for kpl.keysInProcess[key] {
		kpl.c.Wait()
	}

	kpl.keysInProcess[key] = true
	kpl.c.L.Unlock()
}

func (kpl *KopiaPersisterLight) doneWith(key string) {
	kpl.c.L.Lock()
	delete(kpl.keysInProcess, key)
	kpl.c.L.Unlock()
	kpl.c.Signal()
}
