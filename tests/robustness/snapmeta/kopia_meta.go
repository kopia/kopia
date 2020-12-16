package snapmeta

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var _ Persister = &kopiaMetadata{}

// kopiaMetadata handles metadata persistency of a snapshot store, using a Kopia
// repository as the persistency mechanism.
type kopiaMetadata struct {
	*Simple
	localMetadataDir string
	persistenceDir   string
	snap             *kopiarunner.KopiaSnapshotter
}

// New instantiates a new Persister and returns it.
func New(baseDir string) (Persister, error) {
	localDir, err := ioutil.TempDir(baseDir, "kopia-local-metadata-")
	if err != nil {
		return nil, err
	}

	snap, err := kopiarunner.NewKopiaSnapshotter(localDir)
	if err != nil {
		return nil, err
	}

	persistenceDir, err := ioutil.TempDir(localDir, "kopia-persistence-root")
	if err != nil {
		return nil, err
	}

	return &kopiaMetadata{
		localMetadataDir: localDir,
		persistenceDir:   persistenceDir,
		Simple:           NewSimple(),
		snap:             snap,
	}, nil
}

// Cleanup cleans up the local temporary files used by a KopiaMetadata.
func (store *kopiaMetadata) Cleanup() {
	if store.localMetadataDir != "" {
		os.RemoveAll(store.localMetadataDir) //nolint:errcheck
	}

	if store.snap != nil {
		store.snap.Cleanup()
	}
}

// ConnectOrCreateS3 implements the RepoManager interface, connects to a repo in an S3
// bucket or attempts to create one if connection is unsuccessful.
func (store *kopiaMetadata) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	return store.snap.ConnectOrCreateS3(bucketName, pathPrefix)
}

// ConnectOrCreateFilesystem implements the RepoManager interface, connects to a repo in the filesystem
// or attempts to create one if connection is unsuccessful.
func (store *kopiaMetadata) ConnectOrCreateFilesystem(path string) error {
	return store.snap.ConnectOrCreateFilesystem(path)
}

const metadataStoreFileName = "metadata-store-latest"

// ConnectOrCreateS3WithServer implements the RepoManager interface, creates a server
// connects it a repo in an S3 bucket and creates a client to perform operations.
func (store *kopiaMetadata) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, error) {
	return store.snap.ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix)
}

// ConnectOrCreateFilesystemWithServer implements the RepoManager interface, creates a server
// connects it a repo in the filesystem and creates a client to perform operations.
func (store *kopiaMetadata) ConnectOrCreateFilesystemWithServer(repoPath, serverAddr string) (*exec.Cmd, error) {
	return store.snap.ConnectOrCreateFilesystemWithServer(repoPath, serverAddr)
}

// LoadMetadata implements the DataPersister interface, restores the latest
// snapshot from the kopia repository and decodes its contents, populating
// its metadata on the snapshots residing in the target test repository.
func (store *kopiaMetadata) LoadMetadata() error {
	snapIDs, err := store.snap.ListSnapshots()
	if err != nil {
		return err
	}

	if len(snapIDs) == 0 {
		return nil // No snapshot IDs fouund in repository
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
func (store *kopiaMetadata) GetPersistDir() string {
	return store.persistenceDir
}

// FlushMetadata implements the DataPersister interface, flushing the local
// metadata on the target test repo's snapshots to the metadata Kopia repository
// as a snapshot create.
func (store *kopiaMetadata) FlushMetadata() error {
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
