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
// is closed by the same goroutine that opened it (no second cleanup goroutine).
//
// os.Open is called via a goroutine so the caller can return on context
// cancellation. The cost is one goroutine + one buffered channel per call,
// which adds up across million-file snapshots — but the alternative (a stuck
// os.Open blocking the workshare worker indefinitely on locked Windows files)
// caused the 17h hang this PR exists to fix. Cross-platform path here keeps
// the same semantics on all OSes; a future optimization could specialize a
// non-Windows fast path that avoids the goroutine when os.Open is known not
// to hang.
func openWithContext(ctx context.Context, path string) (*os.File, error) {
	// Fail fast on an already-cancelled context — saves the goroutine entirely.
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck
	}

	// If the caller didn't supply a deadline, install a default so a wedged
	// open can't block forever. Done before the goroutine launch so the
	// goroutine inherits the same deadline via the select below.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultFileOpenTimeout)
		defer cancel()
	}

	type result struct {
		f   *os.File
		err error
	}

	ch := make(chan result, 1)

	// `done` lets the goroutine know when the caller has stopped waiting.
	// This must be closed regardless of which select branch returns, so
	// the goroutine can take ownership of the descriptor and close it
	// instead of leaking it through the buffered channel.
	done := make(chan struct{})
	defer close(done)

	go func() {
		f, err := os.Open(path) //nolint:gosec
		select {
		case ch <- result{f, err}:
			// Delivered to the caller; caller owns the descriptor.
		case <-done:
			// Caller already returned via ctx.Done(). We own the cleanup.
			if f != nil {
				f.Close() //nolint:errcheck
			}
		}
	}()

	select {
	case r := <-ch:
		return r.f, r.err
	case <-ctx.Done():
		return nil, ctx.Err() //nolint:wrapcheck
	}
}
