package content

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

const (
	simpleIndexSuffix = ".sndx"
)

type diskCommittedContentIndexCache struct {
	dirname              string
	timeNow              func() time.Time
	v1PerContentOverhead func() int
	log                  *contentlog.Logger
	minSweepAge          time.Duration
}

func (c *diskCommittedContentIndexCache) indexBlobPath(indexBlobID blob.ID) string {
	return filepath.Join(c.dirname, string(indexBlobID)+simpleIndexSuffix)
}

func (c *diskCommittedContentIndexCache) openIndex(ctx context.Context, indexBlobID blob.ID) (index.Index, error) {
	fullpath := c.indexBlobPath(indexBlobID)

	f, closeMmap, err := c.mmapOpenWithRetry(ctx, fullpath)
	if err != nil {
		return nil, err
	}

	ndx, err := index.Open(f, closeMmap, c.v1PerContentOverhead)
	if err != nil {
		closeMmap() //nolint:errcheck
		return nil, errors.Wrapf(err, "error opening index from %v", indexBlobID)
	}

	return ndx, nil
}

// mmapOpenWithRetry attempts mmap.Open() with exponential back-off to work around rare issue specific to Windows where
// we can't open the file right after it has been written.
func (c *diskCommittedContentIndexCache) mmapOpenWithRetry(ctx context.Context, path string) (mmap.MMap, func() error, error) {
	const (
		maxRetries    = 8
		startingDelay = 10 * time.Millisecond
	)

	// retry milliseconds: 10, 20, 40, 80, 160, 320, 640, 1280, total ~2.5s
	f, err := os.Open(path) //nolint:gosec
	nextDelay := startingDelay

	retryCount := 0
	for err != nil && retryCount < maxRetries {
		retryCount++
		contentlog.Log2(ctx, c.log, "retry #%v unable to mmap.Open()",
			logparam.Int("retryCount", retryCount),
			logparam.Error("err", err))
		time.Sleep(nextDelay)
		nextDelay *= 2

		f, err = os.Open(path) //nolint:gosec
	}

	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to open file despite retries")
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close() //nolint:errcheck

		return nil, nil, errors.Wrap(err, "mmap error")
	}

	return mm, func() error {
		if err2 := mm.Unmap(); err2 != nil {
			return errors.Wrapf(err2, "error unmapping index %v", path)
		}

		if err2 := f.Close(); err2 != nil {
			return errors.Wrapf(err2, "error closing index %v", path)
		}

		return nil
	}, nil
}

func (c *diskCommittedContentIndexCache) hasIndexBlobID(_ context.Context, indexBlobID blob.ID) (bool, error) {
	_, err := os.Stat(c.indexBlobPath(indexBlobID))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, errors.Wrapf(err, "error checking %v", indexBlobID)
}

func (c *diskCommittedContentIndexCache) addContentToCache(ctx context.Context, indexBlobID blob.ID, data gather.Bytes) error {
	exists, err := c.hasIndexBlobID(ctx, indexBlobID)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	tmpFile, err := writeTempFileAtomic(c.dirname, data.ToByteSlice())
	if err != nil {
		return err
	}

	// rename() is atomic, so one process will succeed, but the other will fail
	if err := os.Rename(tmpFile, c.indexBlobPath(indexBlobID)); err != nil {
		// verify that the content exists
		exists, err := c.hasIndexBlobID(ctx, indexBlobID)
		if err != nil {
			return err
		}

		if !exists {
			return errors.Errorf("unsuccessful index write of content %q", indexBlobID)
		}
	}

	return nil
}

func writeTempFileAtomic(dirname string, data []byte) (string, error) {
	// write to a temp file to avoid race where two processes are writing at the same time.
	tf, err := os.CreateTemp(dirname, "tmp")
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirname, cache.DirMode) //nolint:errcheck
			tf, err = os.CreateTemp(dirname, "tmp")
		}
	}

	if err != nil {
		return "", errors.Wrap(err, "can't create tmp file")
	}

	if _, err := tf.Write(data); err != nil {
		return "", errors.Wrap(err, "can't write to temp file")
	}

	if err := tf.Close(); err != nil {
		return "", errors.New("can't close tmp file")
	}

	return tf.Name(), nil
}

func (c *diskCommittedContentIndexCache) expireUnused(ctx context.Context, used []blob.ID) error {
	contentlog.Log2(ctx, c.log, "expireUnused",
		blobparam.BlobIDList("except", used),
		logparam.Duration("minSweepAge", c.minSweepAge))

	entries, err := os.ReadDir(c.dirname)
	if err != nil {
		return errors.Wrap(err, "can't list cache")
	}

	remaining := map[blob.ID]os.FileInfo{}

	for _, ent := range entries {
		fi, err := ent.Info()
		if os.IsNotExist(err) {
			// we lost the race, the file was deleted since it was listed.
			continue
		}

		if err != nil {
			return errors.Wrap(err, "failed to read file info")
		}

		if strings.HasSuffix(ent.Name(), simpleIndexSuffix) {
			n := strings.TrimSuffix(ent.Name(), simpleIndexSuffix)
			remaining[blob.ID(n)] = fi
		}
	}

	for _, u := range used {
		delete(remaining, u)
	}

	for _, rem := range remaining {
		if c.timeNow().Sub(rem.ModTime()) > c.minSweepAge {
			contentlog.Log2(ctx, c.log, "removing unused",
				logparam.String("name", rem.Name()),
				logparam.Time("mtime", rem.ModTime()))

			if err := os.Remove(filepath.Join(c.dirname, rem.Name())); err != nil {
				contentlog.Log1(ctx, c.log,
					"unable to remove unused index file",
					logparam.Error("err", err))
			}
		} else {
			contentlog.Log3(ctx, c.log, "keeping unused index because it's too new",
				logparam.String("name", rem.Name()),
				logparam.Time("mtime", rem.ModTime()),
				logparam.Duration("threshold", c.minSweepAge))
		}
	}

	return nil
}
