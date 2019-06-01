// Package webdav implements WebDAV-based Storage.
package webdav

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kopia/kopia/repo/storage"
	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"
)

const (
	davStorageType       = "webdav"
	fsStorageChunkSuffix = ".f"
)

var (
	fsDefaultShards = []int{3, 3}
)

// davStorage implements blob.Storage on top of remove WebDAV repository.
// It is very similar to File storage, except uses HTTP URLs instead of local files.
// Storage formats are compatible (both use sharded directory structure), so a repository
// may be accessed using WebDAV or File interchangeably.
type davStorage struct {
	Options

	cli *gowebdav.Client
}

func (d *davStorage) GetBlock(ctx context.Context, blockID string, offset, length int64) ([]byte, error) {
	_, path := d.getDirPathAndFilePath(blockID)

	data, err := d.cli.Read(path)
	if err != nil {
		return nil, d.translateError(err)
	}
	if length < 0 {
		return data, nil
	}

	if int(offset) > len(data) || offset < 0 {
		return nil, errors.New("invalid offset")
	}

	data = data[offset:]
	if int(length) > len(data) {
		return nil, errors.New("invalid length")
	}

	return data[0:length], nil
}

func (d *davStorage) translateError(err error) error {
	switch err := err.(type) {
	case *os.PathError:
		switch err.Err.Error() {
		case "404":
			return storage.ErrBlockNotFound
		}
		return err
	default:
		return err
	}
}

func getBlockIDFromFileName(name string) (string, bool) {
	if strings.HasSuffix(name, fsStorageChunkSuffix) {
		return name[0 : len(name)-len(fsStorageChunkSuffix)], true
	}

	return "", false
}

func makeFileName(blockID string) string {
	return blockID + fsStorageChunkSuffix
}

func (d *davStorage) ListBlocks(ctx context.Context, prefix string, callback func(storage.BlockMetadata) error) error {
	var walkDir func(string, string) error

	walkDir = func(path string, currentPrefix string) error {
		entries, err := d.cli.ReadDir(gowebdav.FixSlash(path))
		if err != nil {
			return errors.Wrapf(err, "read dir error on %v", path)
		}

		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})

		for _, e := range entries {
			if e.IsDir() {
				newPrefix := currentPrefix + e.Name()
				var match bool

				if len(prefix) > len(newPrefix) {
					// looking for 'abcd', got 'ab' so far, worth trying
					match = strings.HasPrefix(prefix, newPrefix)
				} else {
					match = strings.HasPrefix(newPrefix, prefix)
				}

				if match {
					if err := walkDir(path+"/"+e.Name(), currentPrefix+e.Name()); err != nil {
						return err
					}
				}
			} else if fullID, ok := getBlockIDFromFileName(currentPrefix + e.Name()); ok {
				if strings.HasPrefix(fullID, prefix) {
					if err := callback(storage.BlockMetadata{
						BlockID:   fullID,
						Length:    e.Size(),
						Timestamp: e.ModTime(),
					}); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	return walkDir("", "")
}

func (d *davStorage) PutBlock(ctx context.Context, blockID string, data []byte) error {
	dirPath, filePath := d.getDirPathAndFilePath(blockID)
	tmpPath := fmt.Sprintf("%v-%v", filePath, rand.Int63())
	if err := d.translateError(d.cli.Write(tmpPath, data, 0600)); err != nil {
		if err != storage.ErrBlockNotFound {
			return err
		}

		d.cli.MkdirAll(dirPath, 0700) //nolint:errcheck
		if err = d.translateError(d.cli.Write(tmpPath, data, 0600)); err != nil {
			return err
		}
	}

	return d.translateError(d.cli.Rename(tmpPath, filePath, true))
}

func (d *davStorage) DeleteBlock(ctx context.Context, blockID string) error {
	_, filePath := d.getDirPathAndFilePath(blockID)
	return d.translateError(d.cli.Remove(filePath))
}

func (d *davStorage) getShardDirectory(blockID string) (string, string) {
	shardPath := "/"
	if len(blockID) < 20 {
		return shardPath, blockID
	}
	for _, size := range d.shards() {
		shardPath = filepath.Join(shardPath, blockID[0:size])
		blockID = blockID[size:]
	}

	return shardPath, blockID
}

func (d *davStorage) getDirPathAndFilePath(blockID string) (string, string) {
	shardPath, blockID := d.getShardDirectory(blockID)
	result := filepath.Join(shardPath, makeFileName(blockID))
	return shardPath, result
}

func (d *davStorage) ConnectionInfo() storage.ConnectionInfo {
	return storage.ConnectionInfo{
		Type:   davStorageType,
		Config: &d.Options,
	}
}

func (d *davStorage) Close(ctx context.Context) error {
	return nil
}

// New creates new WebDAV-backed storage in a specified URL.
func New(ctx context.Context, opts *Options) (storage.Storage, error) {
	r := &davStorage{
		Options: *opts,
		cli:     gowebdav.NewClient(opts.URL, opts.Username, opts.Password),
	}

	for _, s := range r.shards() {
		if s == 0 {
			return nil, errors.Errorf("invalid shard spec: %v", opts.DirectoryShards)
		}
	}

	r.Options.URL = strings.TrimSuffix(r.Options.URL, "/")
	return r, nil
}

func init() {
	storage.AddSupportedStorage(
		davStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (storage.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
