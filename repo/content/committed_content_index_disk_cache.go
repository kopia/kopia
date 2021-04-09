package content

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	"github.com/kopia/kopia/repo/blob"
)

const (
	simpleIndexSuffix                      = ".sndx"
	unusedCommittedContentIndexCleanupTime = 1 * time.Hour // delete unused committed index blobs after 1 hour
)

type diskCommittedContentIndexCache struct {
	dirname string
	timeNow func() time.Time
}

func (c *diskCommittedContentIndexCache) indexBlobPath(indexBlobID blob.ID) string {
	return filepath.Join(c.dirname, string(indexBlobID)+simpleIndexSuffix)
}

func (c *diskCommittedContentIndexCache) openIndex(ctx context.Context, indexBlobID blob.ID) (packIndex, error) {
	fullpath := c.indexBlobPath(indexBlobID)

	f, err := mmapOpenWithRetry(ctx, fullpath)
	if err != nil {
		return nil, err
	}

	return openPackIndex(f)
}

// mmapOpenWithRetry attempts mmap.Open() with exponential back-off to work around rare issue specific to Windows where
// we can't open the file right after it has been written.
func mmapOpenWithRetry(ctx context.Context, path string) (*mmap.ReaderAt, error) {
	const (
		maxRetries    = 8
		startingDelay = 10 * time.Millisecond
	)

	// retry milliseconds: 10, 20, 40, 80, 160, 320, 640, 1280, total ~2.5s
	f, err := mmap.Open(path)
	nextDelay := startingDelay

	retryCount := 0
	for err != nil && retryCount < maxRetries {
		retryCount++
		log(ctx).Debugf("retry #%v unable to mmap.Open(): %v", retryCount, err)
		time.Sleep(nextDelay)
		nextDelay *= 2
		f, err = mmap.Open(path)
	}

	return f, errors.Wrap(err, "mmap() error")
}

func (c *diskCommittedContentIndexCache) hasIndexBlobID(ctx context.Context, indexBlobID blob.ID) (bool, error) {
	_, err := os.Stat(c.indexBlobPath(indexBlobID))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, errors.Wrapf(err, "error checking %v", indexBlobID)
}

func (c *diskCommittedContentIndexCache) addContentToCache(ctx context.Context, indexBlobID blob.ID, data []byte) error {
	exists, err := c.hasIndexBlobID(ctx, indexBlobID)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	tmpFile, err := writeTempFileAtomic(c.dirname, data)
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
	tf, err := ioutil.TempFile(dirname, "tmp")
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirname, 0o700) //nolint:errcheck
			tf, err = ioutil.TempFile(dirname, "tmp")
		}
	}

	if err != nil {
		return "", errors.Wrap(err, "can't create tmp file")
	}

	if _, err := tf.Write(data); err != nil {
		return "", errors.Wrap(err, "can't write to temp file")
	}

	if err := tf.Close(); err != nil {
		return "", errors.Errorf("can't close tmp file")
	}

	return tf.Name(), nil
}

func (c *diskCommittedContentIndexCache) expireUnused(ctx context.Context, used []blob.ID) error {
	entries, err := ioutil.ReadDir(c.dirname)
	if err != nil {
		return errors.Wrap(err, "can't list cache")
	}

	remaining := map[blob.ID]os.FileInfo{}

	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), simpleIndexSuffix) {
			n := strings.TrimSuffix(ent.Name(), simpleIndexSuffix)
			remaining[blob.ID(n)] = ent
		}
	}

	for _, u := range used {
		delete(remaining, u)
	}

	for _, rem := range remaining {
		if c.timeNow().Sub(rem.ModTime()) > unusedCommittedContentIndexCleanupTime {
			log(ctx).Debugf("removing unused %v %v", rem.Name(), rem.ModTime())

			if err := os.Remove(filepath.Join(c.dirname, rem.Name())); err != nil {
				log(ctx).Errorf("unable to remove unused index file: %v", err)
			}
		} else {
			log(ctx).Debugf("keeping unused %v because it's too new %v", rem.Name(), rem.ModTime())
		}
	}

	return nil
}
