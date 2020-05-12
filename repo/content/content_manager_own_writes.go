package content

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

const ownWritesCacheRetention = 15 * time.Minute

type ownWritesCache interface {
	add(ctx context.Context, mb blob.Metadata) error
	merge(ctx context.Context, prefix blob.ID, source []blob.Metadata) ([]blob.Metadata, error)
	delete(ctx context.Context, md blob.ID) error
}

// nullOwnWritesCache is an implementation of ownWritesCache that ignores all changes.
type nullOwnWritesCache struct {
}

func (n *nullOwnWritesCache) add(ctx context.Context, mb blob.Metadata) error {
	return nil
}

func (n *nullOwnWritesCache) delete(ctx context.Context, blobID blob.ID) error {
	return nil
}

func (n *nullOwnWritesCache) merge(ctx context.Context, prefix blob.ID, source []blob.Metadata) ([]blob.Metadata, error) {
	return source, nil
}

// memoryOwnWritesCache is an implementation of ownWritesCache that caches in memory.
type memoryOwnWritesCache struct {
	entries sync.Map
	timeNow func() time.Time
}

func (n *memoryOwnWritesCache) add(ctx context.Context, mb blob.Metadata) error {
	log(ctx).Debugf("adding %v to own-writes cache", mb.BlobID)
	n.entries.Store(mb.BlobID, mb)

	return nil
}

func (n *memoryOwnWritesCache) delete(ctx context.Context, blobID blob.ID) error {
	return n.add(ctx, blob.Metadata{
		BlobID:    blobID,
		Length:    -1,
		Timestamp: n.timeNow(),
	})
}

func (n *memoryOwnWritesCache) merge(ctx context.Context, prefix blob.ID, source []blob.Metadata) ([]blob.Metadata, error) {
	var result []blob.Metadata

	n.entries.Range(func(key, value interface{}) bool {
		md := value.(blob.Metadata)
		if !strings.HasPrefix(string(md.BlobID), string(prefix)) {
			return true
		}

		if age := n.timeNow().Sub(md.Timestamp); age < ownWritesCacheRetention {
			result = append(result, md)
		} else {
			log(ctx).Debugf("deleting stale own writes cache entry: %v (%v)", key, age)

			n.entries.Delete(key)
		}

		return true
	})

	return mergeOwnWrites(ctx, source, result), nil
}

// persistentOwnWritesCache is an implementation of ownWritesCache that caches entries to strongly consistent blob storage.
type persistentOwnWritesCache struct {
	st      blob.Storage
	timeNow func() time.Time
}

func (d *persistentOwnWritesCache) add(ctx context.Context, mb blob.Metadata) error {
	j, err := json.Marshal(mb)
	if err != nil {
		return errors.Wrap(err, "unable to marshal JSON")
	}

	return d.st.PutBlob(ctx, mb.BlobID, gather.FromSlice(j))
}

func (d *persistentOwnWritesCache) merge(ctx context.Context, prefix blob.ID, source []blob.Metadata) ([]blob.Metadata, error) {
	var myWrites []blob.Metadata

	err := d.st.ListBlobs(ctx, prefix, func(md blob.Metadata) error {
		b, err := d.st.GetBlob(ctx, md.BlobID, 0, -1)
		if err == blob.ErrBlobNotFound {
			return nil
		}

		if err != nil {
			return errors.Wrapf(err, "error reading own write cache entry %v", md.BlobID)
		}

		var originalMD blob.Metadata

		if err := json.Unmarshal(b, &originalMD); err != nil {
			return errors.Wrapf(err, "error unmarshaling own write cache entry %v", md.BlobID)
		}

		if age := d.timeNow().Sub(originalMD.Timestamp); age < ownWritesCacheRetention {
			myWrites = append(myWrites, originalMD)
		} else {
			log(ctx).Debugf("deleting blob %v from own-write cache because it's too old: %v", age, d.timeNow(), originalMD.Timestamp)

			if err := d.st.DeleteBlob(ctx, md.BlobID); err != nil && err != blob.ErrBlobNotFound {
				return errors.Wrap(err, "error deleting stale blob")
			}
		}

		return nil
	})

	return mergeOwnWrites(ctx, source, myWrites), err
}

func (d *persistentOwnWritesCache) delete(ctx context.Context, blobID blob.ID) error {
	return d.add(ctx, blob.Metadata{
		BlobID:    blobID,
		Length:    -1,
		Timestamp: d.timeNow(),
	})
}

func mergeOwnWrites(ctx context.Context, source, own []blob.Metadata) []blob.Metadata {
	m := map[blob.ID]blob.Metadata{}

	for _, v := range source {
		m[v.BlobID] = v
	}

	for _, v := range own {
		if v.Length < 0 {
			delete(m, v.BlobID)
		} else {
			m[v.BlobID] = v
		}
	}

	var s []blob.Metadata

	for _, v := range m {
		s = append(s, v)
	}

	log(ctx).Debugf("merged %v backend blobs and %v local blobs into %v", len(source), len(own), len(s))

	return s
}

func newOwnWritesCache(ctx context.Context, caching *CachingOptions, timeNow func() time.Time) (ownWritesCache, error) {
	if caching.CacheDirectory == "" {
		return &memoryOwnWritesCache{timeNow: timeNow}, nil
	}

	dirname := filepath.Join(caching.CacheDirectory, "own-writes")

	if err := os.MkdirAll(dirname, 0700); err != nil {
		return nil, errors.Wrap(err, "unable to create own writes cache directory")
	}

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path:            dirname,
		DirectoryShards: []int{},
	})

	if err != nil {
		return nil, errors.Wrap(err, "unable to create own writes cache storage")
	}

	return &persistentOwnWritesCache{st, timeNow}, nil
}
