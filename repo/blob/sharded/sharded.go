// Package sharded implements common support for sharded blob providers, such as filesystem or webdav.
package sharded

import (
	"context"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

// CompleteBlobSuffix is the extension for sharded blobs that have completed writing.
const CompleteBlobSuffix = ".f"

var log = logging.Module("sharded") // +checklocksignore

// Impl must be implemented by underlying provider.
type Impl interface {
	GetBlobFromPath(ctx context.Context, dirPath, filePath string, offset, length int64, output blob.OutputBuffer) error
	GetMetadataFromPath(ctx context.Context, dirPath, filePath string) (blob.Metadata, error)
	PutBlobInPath(ctx context.Context, dirPath, filePath string, dataSlices blob.Bytes, opts blob.PutOptions) error
	DeleteBlobInPath(ctx context.Context, dirPath, filePath string) error
	ReadDir(ctx context.Context, path string) ([]os.FileInfo, error)
}

// Storage provides common implementation of sharded storage.
type Storage struct {
	Impl Impl

	RootPath string
	Options

	parametersMutex sync.Mutex

	// +checklocks:parametersMutex
	parameters *Parameters
}

// GetBlob implements blob.Storage.
func (s *Storage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64, output blob.OutputBuffer) error {
	dirPath, filePath, err := s.GetShardedPathAndFilePath(ctx, blobID)
	if err != nil {
		return errors.Wrap(err, "error determining sharded path")
	}

	//nolint:wrapcheck
	return s.Impl.GetBlobFromPath(ctx, dirPath, filePath, offset, length, output)
}

func (s *Storage) getBlobIDFromFileName(name string) (blob.ID, bool) {
	if strings.HasSuffix(name, CompleteBlobSuffix) {
		return blob.ID(name[0 : len(name)-len(CompleteBlobSuffix)]), true
	}

	return blob.ID(""), false
}

func (s *Storage) makeFileName(blobID blob.ID) string {
	return string(blobID) + CompleteBlobSuffix
}

// ListBlobs implements blob.Storage.
func (s *Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	pw := parallelwork.NewQueue()

	// channel to which pw will write blob.Metadata, some buf
	result := make(chan blob.Metadata, 128) //nolint:mnd

	finished := make(chan struct{})
	defer close(finished)

	var walkDir func(string, string) error

	walkDir = func(directory string, currentPrefix string) error {
		select {
		case <-finished: // already finished
			return nil
		default:
		}

		entries, err := s.Impl.ReadDir(ctx, directory)
		if err != nil {
			return errors.Wrap(err, "error reading directory")
		}

		for _, e := range entries {
			if e.IsDir() {
				var match bool

				newPrefix := currentPrefix + e.Name()
				if len(prefix) > len(newPrefix) {
					match = strings.HasPrefix(string(prefix), newPrefix)
				} else {
					match = strings.HasPrefix(newPrefix, string(prefix))
				}

				if match {
					subdir := directory + "/" + e.Name()
					subprefix := currentPrefix + e.Name()

					pw.EnqueueFront(ctx, func() error {
						return walkDir(subdir, subprefix)
					})
				}

				continue
			}

			fullID, ok := s.getBlobIDFromFileName(currentPrefix + e.Name())
			if !ok {
				continue
			}

			if !strings.HasPrefix(string(fullID), string(prefix)) {
				continue
			}

			select {
			case result <- blob.Metadata{
				BlobID:    fullID,
				Length:    e.Size(),
				Timestamp: e.ModTime(),
			}:
			case <-finished:
			}
		}

		return nil
	}

	pw.EnqueueFront(ctx, func() error {
		return walkDir(s.RootPath, "")
	})

	par := s.ListParallelism
	if par == 0 {
		par = 1
	}

	var eg errgroup.Group

	// start populating the channel in parallel
	eg.Go(func() error {
		defer close(result)

		return errors.Wrap(pw.Process(ctx, par), "error processing directory shards")
	})

	// invoke the callback on the current goroutine until it fails
	for bm := range result {
		if err := callback(bm); err != nil {
			return err
		}
	}

	//nolint:wrapcheck
	return eg.Wait()
}

// GetMetadata implements blob.Storage.
func (s *Storage) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	dirPath, filePath, err := s.GetShardedPathAndFilePath(ctx, blobID)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error determining sharded path")
	}

	m, err := s.Impl.GetMetadataFromPath(ctx, dirPath, filePath)
	m.BlobID = blobID

	return m, errors.Wrap(err, "error getting metadata")
}

// PutBlob implements blob.Storage.
func (s *Storage) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	dirPath, filePath, err := s.GetShardedPathAndFilePath(ctx, blobID)
	if err != nil {
		return errors.Wrap(err, "error determining sharded path")
	}

	//nolint:wrapcheck
	return s.Impl.PutBlobInPath(ctx, dirPath, filePath, data, opts)
}

// DeleteBlob implements blob.Storage.
func (s *Storage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	dirPath, filePath, err := s.GetShardedPathAndFilePath(ctx, blobID)
	if err != nil {
		return errors.Wrap(err, "error determining sharded path")
	}

	//nolint:wrapcheck
	return s.Impl.DeleteBlobInPath(ctx, dirPath, filePath)
}

func (s *Storage) getParameters(ctx context.Context) (*Parameters, error) {
	s.parametersMutex.Lock()
	defer s.parametersMutex.Unlock()

	if s.parameters != nil {
		return s.parameters, nil
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	dotShardsFile := path.Join(s.RootPath, ParametersFile)

	//nolint:nestif
	if err := s.Impl.GetBlobFromPath(ctx, s.RootPath, dotShardsFile, 0, -1, &tmp); err != nil {
		if !errors.Is(err, blob.ErrBlobNotFound) {
			return nil, errors.Wrap(err, "error getting sharding parameters for storage")
		}

		// blob.ErrBlobNotFound is ok, initialize parameters from defaults.
		s.parameters = DefaultParameters(s.DirectoryShards)

		tmp.Reset()

		if err := s.parameters.Save(&tmp); err != nil {
			return nil, errors.Wrap(err, "error serializing sharding parameters")
		}

		if err := s.Impl.PutBlobInPath(ctx, s.RootPath, dotShardsFile, tmp.Bytes(), blob.PutOptions{}); err != nil {
			log(ctx).Warnf("unable to persist sharding parameters: %v", err)
		}
	} else {
		par := &Parameters{}

		if err := par.Load(tmp.Bytes().Reader()); err != nil {
			return nil, errors.Wrap(err, "error parsing sharding parameters for storage")
		}

		s.parameters = par
	}

	return s.parameters, nil
}

func (s *Storage) getShardDirectory(ctx context.Context, blobID blob.ID) (string, blob.ID, error) {
	p, err := s.getParameters(ctx)
	if err != nil {
		return "", "", err
	}

	shardedPath, shardedBlob := p.GetShardDirectoryAndBlob(s.RootPath, blobID)

	return shardedPath, shardedBlob, nil
}

// GetShardedPathAndFilePath returns the path of the shard and file name within the shard for a given blob ID.
func (s *Storage) GetShardedPathAndFilePath(ctx context.Context, blobID blob.ID) (shardPath, filePath string, err error) {
	shardPath, blobID, err = s.getShardDirectory(ctx, blobID)
	if err != nil {
		return
	}

	filePath = path.Join(shardPath, s.makeFileName(blobID))

	return
}

// New returns new sharded.Storage helper.
func New(impl Impl, rootPath string, opt Options, isCreate bool) Storage {
	if opt.DirectoryShards == nil {
		if isCreate {
			opt.DirectoryShards = []int{1, 3}
		} else {
			opt.DirectoryShards = []int{3, 3}
		}
	}

	return Storage{
		Impl:     impl,
		RootPath: rootPath,
		Options:  opt,
	}
}
