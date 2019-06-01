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

	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"

	"github.com/kopia/kopia/repo/blob"
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

func (d *davStorage) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64) ([]byte, error) {
	_, path := d.getDirPathAndFilePath(blobID)

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
			return blob.ErrBlobNotFound
		}
		return err
	default:
		return err
	}
}

func getBlobIDFromFilename(name string) (string, bool) {
	if strings.HasSuffix(name, fsStorageChunkSuffix) {
		return name[0 : len(name)-len(fsStorageChunkSuffix)], true
	}

	return "", false
}

func makeFileName(blobID blob.ID) string {
	return string(blobID) + fsStorageChunkSuffix
}

func (d *davStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
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
					match = strings.HasPrefix(string(prefix), newPrefix)
				} else {
					match = strings.HasPrefix(newPrefix, string(prefix))
				}

				if match {
					if err := walkDir(path+"/"+e.Name(), currentPrefix+e.Name()); err != nil {
						return err
					}
				}
			} else if fullID, ok := getBlobIDFromFilename(currentPrefix + e.Name()); ok {
				if strings.HasPrefix(fullID, string(prefix)) {
					if err := callback(blob.Metadata{
						BlobID:    blob.ID(fullID),
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

func (d *davStorage) PutBlob(ctx context.Context, blobID blob.ID, data []byte) error {
	dirPath, filePath := d.getDirPathAndFilePath(blobID)
	tmpPath := fmt.Sprintf("%v-%v", filePath, rand.Int63())
	if err := d.translateError(d.cli.Write(tmpPath, data, 0600)); err != nil {
		if err != blob.ErrBlobNotFound {
			return err
		}

		d.cli.MkdirAll(dirPath, 0700) //nolint:errcheck
		if err = d.translateError(d.cli.Write(tmpPath, data, 0600)); err != nil {
			return err
		}
	}

	return d.translateError(d.cli.Rename(tmpPath, filePath, true))
}

func (d *davStorage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	_, filePath := d.getDirPathAndFilePath(blobID)
	return d.translateError(d.cli.Remove(filePath))
}

func (d *davStorage) getShardDirectory(blobID blob.ID) (string, blob.ID) {
	shardPath := "/"
	if len(blobID) < 20 {
		return shardPath, blobID
	}
	for _, size := range d.shards() {
		shardPath = filepath.Join(shardPath, string(blobID[0:size]))
		blobID = blobID[size:]
	}

	return shardPath, blobID
}

func (d *davStorage) getDirPathAndFilePath(blobID blob.ID) (string, string) {
	shardPath, blobID := d.getShardDirectory(blobID)
	result := filepath.Join(shardPath, makeFileName(blobID))
	return shardPath, result
}

func (d *davStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   davStorageType,
		Config: &d.Options,
	}
}

func (d *davStorage) Close(ctx context.Context) error {
	return nil
}

// New creates new WebDAV-backed storage in a specified URL.
func New(ctx context.Context, opts *Options) (blob.Storage, error) {
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
	blob.AddSupportedStorage(
		davStorageType,
		func() interface{} { return &Options{} },
		func(ctx context.Context, o interface{}) (blob.Storage, error) {
			return New(ctx, o.(*Options))
		})
}
