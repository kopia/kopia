package localfs

import (
	"context"
	"os"
	"time"
)

// defaultFileOpenTimeout is the maximum time to wait for os.Open to complete.
// This prevents indefinite blocking on locked or inaccessible files from
// stalling workshare pool workers, which can cascade into a full snapshot hang.
const defaultFileOpenTimeout = 60 * time.Second

// openWithContext opens a file, respecting context cancellation and deadlines.
// If the context has no deadline, a default timeout is applied. If os.Open blocks
// beyond the deadline, an error is returned and any eventually-opened file handle
// is closed in the background.
func openWithContext(ctx context.Context, path string) (*os.File, error) {
	// Fast path: try non-blocking open first. If it completes immediately
	// (the common case), avoid the goroutine overhead entirely.
	type result struct {
		f   *os.File
		err error
	}

	ch := make(chan result, 1)

	go func() {
		f, err := os.Open(path) //nolint:gosec
		ch <- result{f, err}
	}()

	// If context has no deadline, add a default timeout to prevent
	// indefinite blocking on locked files.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultFileOpenTimeout)
		defer cancel()
	}

	select {
	case r := <-ch:
		return r.f, r.err
	case <-ctx.Done():
		// os.Open is still pending. Start a cleanup goroutine that waits for
		// the open to complete and closes the handle if it eventually succeeds.
		go func() {
			r := <-ch
			if r.f != nil {
				r.f.Close() //nolint:errcheck
			}
		}()

		return nil, ctx.Err() //nolint:wrapcheck
	}
}
