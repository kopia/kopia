// +build darwin,amd64 linux,amd64

// Package kopiaclient provides a client to interact with a Kopia repo.
package kopiaclient

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/s3"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/tests/robustness"
)

// KopiaClient uses a Kopia repo to create, restore, and delete snapshots.
type KopiaClient struct {
	configPath string
	pw         string
}

const (
	configFileName           = "config"
	password                 = "kj13498po&_EXAMPLE" //nolint:gosec
	s3Endpoint               = "s3.amazonaws.com"
	awsAccessKeyIDEnvKey     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyEnvKey = "AWS_SECRET_ACCESS_KEY" //nolint:gosec
)

// NewKopiaClient returns a new KopiaClient.
func NewKopiaClient(basePath string) *KopiaClient {
	return &KopiaClient{
		configPath: filepath.Join(basePath, configFileName),
		pw:         password,
	}
}

// CreateOrConnectRepo creates a new Kopia repo or connects to an existing one if possible.
func (kc *KopiaClient) CreateOrConnectRepo(ctx context.Context, repoDir, bucketName string) error {
	st, err := kc.getStorage(ctx, repoDir, bucketName)
	if err != nil {
		return err
	}

	if iErr := repo.Initialize(ctx, st, &repo.NewRepositoryOptions{}, kc.pw); iErr != nil {
		if !errors.Is(iErr, repo.ErrAlreadyInitialized) {
			return errors.Wrap(iErr, "repo is already initialized")
		}

		log.Println("connecting to existing repository")
	}

	if iErr := repo.Connect(ctx, kc.configPath, st, kc.pw, &repo.ConnectOptions{}); iErr != nil {
		return errors.Wrap(iErr, "error connecting to repository")
	}

	return errors.Wrap(err, "unable to open repository")
}

// SnapshotCreate creates a snapshot for the given path.
func (kc *KopiaClient) SnapshotCreate(ctx context.Context, path string) error {
	r, err := repo.Open(ctx, kc.configPath, kc.pw, &repo.Options{})
	if err != nil {
		return errors.Wrap(err, "cannot open repository")
	}

	rw, err := r.NewWriter(ctx, repo.WriteSessionOptions{})
	if err != nil {
		return errors.Wrap(err, "cannot get new repository writer")
	}

	si, err := kc.getSourceInfoFromPath(r, filepath.Base(path))
	if err != nil {
		return errors.Wrap(err, "cannot get source info from path")
	}

	policyTree, err := policy.TreeForSource(ctx, r, si)
	if err != nil {
		return errors.Wrap(err, "cannot get policy tree for source")
	}

	source, err := localfs.NewEntry(path)
	if err != nil {
		return errors.Wrap(err, "cannot get filesystem entry from path")
	}

	u := snapshotfs.NewUploader(rw)

	man, err := u.Upload(ctx, source, policyTree, si)
	if err != nil {
		return errors.Wrap(err, "cannot get manifest")
	}

	log.Printf("snapshotting %v", units.BytesStringBase10(man.Stats.TotalFileSize))

	if _, err := snapshot.SaveSnapshot(ctx, rw, man); err != nil {
		return errors.Wrap(err, "cannot save snapshot")
	}

	if err := rw.Flush(ctx); err != nil {
		return err
	}

	return r.Close(ctx)
}

// SnapshotRestore restores the latest snapshot for the given path.
func (kc *KopiaClient) SnapshotRestore(ctx context.Context, path string) error {
	r, err := repo.Open(ctx, kc.configPath, kc.pw, &repo.Options{})
	if err != nil {
		return errors.Wrap(err, "cannot open repository")
	}

	mans, err := kc.getSnapshotsFromPath(ctx, r, filepath.Base(path))
	if err != nil {
		return err
	}

	man := kc.latestManifest(mans)

	rootEntry, err := snapshotfs.FilesystemEntryFromIDWithPath(ctx, r, string(man.ID), false)
	if err != nil {
		return errors.Wrap(err, "cannot get filesystem entry from ID with path")
	}

	output := &restore.FilesystemOutput{TargetPath: path}

	st, err := restore.Entry(ctx, r, output, rootEntry, restore.Options{})
	if err != nil {
		return errors.Wrap(err, "cannot restore snapshot")
	}

	log.Printf("restored %v", units.BytesStringBase10(st.RestoredTotalFileSize))

	return r.Close(ctx)
}

// SnapshotDelete deletes all snapshots for a given path.
func (kc *KopiaClient) SnapshotDelete(ctx context.Context, path string) error {
	r, err := repo.Open(ctx, kc.configPath, kc.pw, &repo.Options{})
	if err != nil {
		return errors.Wrap(err, "cannot open repository")
	}

	rw, err := r.NewWriter(ctx, repo.WriteSessionOptions{})
	if err != nil {
		return errors.Wrap(err, "cannot get new repository writer")
	}

	mans, err := kc.getSnapshotsFromPath(ctx, r, filepath.Base(path))
	if err != nil {
		return err
	}

	for _, man := range mans {
		err = rw.DeleteManifest(ctx, man.ID)
		if err != nil {
			return errors.Wrap(err, "cannot delete manifest")
		}
	}

	if err := rw.Flush(ctx); err != nil {
		return err
	}

	return r.Close(ctx)
}

func (kc *KopiaClient) getStorage(ctx context.Context, repoDir, bucketName string) (st blob.Storage, err error) {
	if bucketName != "" {
		s3Opts := &s3.Options{
			BucketName:      bucketName,
			Prefix:          repoDir,
			Endpoint:        s3Endpoint,
			AccessKeyID:     os.Getenv(awsAccessKeyIDEnvKey),
			SecretAccessKey: os.Getenv(awsSecretAccessKeyEnvKey),
		}
		st, err = s3.New(ctx, s3Opts)
	} else {
		if iErr := os.MkdirAll(repoDir, 0o700); iErr != nil {
			return nil, errors.Wrap(iErr, "cannot create directory")
		}

		fsOpts := &filesystem.Options{
			Path: repoDir,
		}
		st, err = filesystem.New(ctx, fsOpts)
	}

	return st, errors.Wrap(err, "unable to get storage")
}

func (kc *KopiaClient) getSnapshotsFromPath(ctx context.Context, r repo.Repository, path string) ([]*snapshot.Manifest, error) {
	si, err := kc.getSourceInfoFromPath(r, path)
	if err != nil {
		return nil, err
	}

	manifests, err := snapshot.ListSnapshots(ctx, r, si)
	if err != nil {
		return nil, errors.Wrap(err, "cannot list snapshots")
	}

	if len(manifests) == 0 {
		return nil, robustness.ErrKeyNotFound
	}

	return manifests, nil
}

func (kc *KopiaClient) getSourceInfoFromPath(r repo.Repository, path string) (snapshot.SourceInfo, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return snapshot.SourceInfo{}, errors.Wrap(err, "cannot get absolute path")
	}

	return snapshot.SourceInfo{
		Host:     r.ClientOptions().Hostname,
		UserName: r.ClientOptions().Username,
		Path:     absPath,
	}, nil
}

func (kc *KopiaClient) latestManifest(mans []*snapshot.Manifest) *snapshot.Manifest {
	latest := mans[0]

	for _, m := range mans {
		if m.StartTime.After(latest.StartTime) {
			latest = m
		}
	}

	return latest
}
