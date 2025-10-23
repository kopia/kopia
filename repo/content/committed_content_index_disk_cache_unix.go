//go:build !windows

package content

import (
	"context"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

// Unix semantics: Close the file descriptor immediately after a successful mmap so the
// process does not retain FDs for all mapped index files. The mapping remains valid until
// Unmap is called.
func (c *diskCommittedContentIndexCache) mmapOpenWithRetry(_ context.Context, path string) (mmap.MMap, func() error, error) {
	// retry milliseconds: 10, 20, 40, 80, 160, 320, 640, 1280, total ~2.5s
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to open file despite retries")
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		_ = f.Close()
		return nil, nil, errors.Wrap(err, "mmap error")
	}

	// On Unix, it's safe to close the FD now; the mapping remains valid.
	if err := f.Close(); err != nil {
		// If close fails, still return mapping, but report error on closer to surface the issue later.
		closeErr := errors.Wrapf(err, "error closing index %v after mmap", path)

		return mm, func() error {
			if err2 := mm.Unmap(); err2 != nil {
				return errors.Wrapf(err2, "error unmapping index %v (also had close error: %v)", path, closeErr)
			}

			return closeErr
		}, nil
	}

	return mm, func() error {
		if err2 := mm.Unmap(); err2 != nil {
			return errors.Wrapf(err2, "error unmapping index %v", path)
		}

		return nil
	}, nil
}
