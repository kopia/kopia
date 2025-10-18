//go:build windows

package content

import (
	"context"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
)

// mmapOpenWithRetry attempts mmap.Open() with exponential back-off to work around a rare issue
// where Windows can't open the file right after it has been written.
//
// Windows semantics: keep the file descriptor open until Unmap due to OS requirements.
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
		contentlog.Log2(ctx, c.log, "retry unable to mmap.Open()",
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
