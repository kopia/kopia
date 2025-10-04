//go:build windows

package content

import (
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

// Windows semantics: keep the file descriptor open until Unmap due to OS requirements.
func (c *diskCommittedContentIndexCache) mmapOpenWithRetry(path string) (mmap.MMap, func() error, error) {
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
		c.log.Debugf("retry #%v unable to mmap.Open(): %v", retryCount, err)
		time.Sleep(nextDelay)
		nextDelay *= 2

		f, err = os.Open(path) //nolint:gosec
	}

	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to open file despite retries")
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		_ = f.Close()
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
