//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package snapmeta provides Kopia implementations of Persister and Snapshotter.
package snapmeta

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kopia/kopia/tests/robustness"
)

// KopiaPersister implements robustness.Persister.
type KopiaPersister struct {
	*Simple
	localMetadataDir string
	persistenceDir   string
	kopiaConnector
}

var _ robustness.Persister = (*KopiaPersister)(nil)

// NewPersister returns a Kopia based Persister.
// ConnectOrCreateRepo must be invoked to enable the interface.
func NewPersister(baseDir string) (*KopiaPersister, error) {
	localDir, err := os.MkdirTemp(baseDir, "kopia-local-metadata-")
	if err != nil {
		return nil, err
	}

	persistenceDir, err := os.MkdirTemp(localDir, "kopia-persistence-root")
	if err != nil {
		return nil, err
	}

	km := &KopiaPersister{
		localMetadataDir: localDir,
		persistenceDir:   persistenceDir,
		Simple:           NewSimple(),
	}

	if err := km.initializeConnector(localDir); err != nil {
		return nil, err
	}

	km.initS3WithServerFn = km.persisterInitS3WithServer
	km.initFilesystemWithServerFn = km.persisterInitFilesystemWithServer

	return km, nil
}

// persisterInitS3WithServer is an adaptor for initS3() as the persister
// does not support the server configuration.
func (store *KopiaPersister) persisterInitS3WithServer(repoPath, bucketName, addr string) error {
	return store.initS3(repoPath, bucketName)
}

// persisterInitFilesystemWithServer is an adaptor for initFilesystem() as the persister
// does not support the server configuration.
func (store *KopiaPersister) persisterInitFilesystemWithServer(repoPath, addr string) error {
	return store.initFilesystem(repoPath)
}

// ConnectOrCreateRepo makes the Persister ready for use.
func (store *KopiaPersister) ConnectOrCreateRepo(repoPath string) error {
	return store.connectOrCreateRepo(repoPath)
}

// Cleanup cleans up the local temporary files used by a KopiaMetadata.
func (store *KopiaPersister) Cleanup() {
	if store.localMetadataDir != "" {
		os.RemoveAll(store.localMetadataDir) //nolint:errcheck
	}

	if store.snap != nil {
		store.snap.Cleanup()
	}
}

// ConnectOrCreateS3 implements the RepoManager interface, connects to a repo in an S3
// bucket or attempts to create one if connection is unsuccessful.
func (store *KopiaPersister) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	return store.snap.ConnectOrCreateS3(bucketName, pathPrefix)
}

// ConnectOrCreateFilesystem implements the RepoManager interface, connects to a repo in the filesystem
// or attempts to create one if connection is unsuccessful.
func (store *KopiaPersister) ConnectOrCreateFilesystem(path string) error {
	return store.snap.ConnectOrCreateFilesystem(path)
}

const metadataStoreFileName = "metadata-store-latest"

// ConnectOrCreateS3WithServer implements the RepoManager interface, creates a server
// connects it a repo in an S3 bucket and creates a client to perform operations.
func (store *KopiaPersister) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, string, error) {
	return store.snap.ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix)
}

// ConnectOrCreateFilesystemWithServer implements the RepoManager interface, creates a server
// connects it a repo in the filesystem and creates a client to perform operations.
func (store *KopiaPersister) ConnectOrCreateFilesystemWithServer(repoPath, serverAddr string) (*exec.Cmd, string, error) {
	return store.snap.ConnectOrCreateFilesystemWithServer(repoPath, serverAddr)
}

// LoadMetadata implements the DataPersister interface, restores the latest
// snapshot from the kopia repository and decodes its contents, populating
// its metadata on the snapshots residing in the target test repository.
func (store *KopiaPersister) LoadMetadata() error {
	snapIDs, err := store.snap.ListSnapshots()
	if err != nil {
		return err
	}

	if len(snapIDs) == 0 {
		return nil // No snapshot IDs found in repository
	}

	lastSnapID := snapIDs[len(snapIDs)-1]

	err = store.snap.RestoreSnapshot(lastSnapID, store.persistenceDir)
	if err != nil {
		return err
	}

	metadataPath := filepath.Join(store.persistenceDir, metadataStoreFileName)

	defer os.Remove(metadataPath) //nolint:errcheck

	f, err := os.Open(metadataPath) //nolint:gosec
	if err != nil {
		return err
	}

	err = json.NewDecoder(f).Decode(&(store.Simple))
	if err != nil {
		return err
	}

	return nil
}

// GetPersistDir returns the path to the directory that will be persisted
// as a snapshot to the kopia repository.
func (store *KopiaPersister) GetPersistDir() string {
	return store.persistenceDir
}

// FlushMetadata implements the DataPersister interface, flushing the local
// metadata on the target test repo's snapshots to the metadata Kopia repository
// as a snapshot create.
func (store *KopiaPersister) FlushMetadata() error {
	metadataPath := filepath.Join(store.persistenceDir, metadataStoreFileName)

	f, err := os.Create(metadataPath)
	if err != nil {
		return err
	}

	defer func() {
		f.Close()               //nolint:errcheck
		os.Remove(metadataPath) //nolint:errcheck
	}()

	err = json.NewEncoder(f).Encode(store.Simple)
	if err != nil {
		return err
	}

	_, err = store.snap.CreateSnapshot(store.persistenceDir)
	if err != nil {
		return err
	}

	return nil
}
