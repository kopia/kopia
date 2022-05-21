package snapshotfs

import (
	"context"
	"path"
	"runtime"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/workshare"
	"github.com/kopia/kopia/repo/object"
)

const walkersPerCPU = 4

// EntryCallback is invoked when walking the tree of snapshots.
type EntryCallback func(ctx context.Context, entry fs.Entry, oid object.ID, entryPath string) error

// TreeWalker processes snapshot filesystem trees by invoking the provided callback
// once for each object found in the tree.
type TreeWalker struct {
	options TreeWalkerOptions

	enqueued sync.Map
	wp       *workshare.Pool

	mu sync.Mutex
	// +checklocks:mu
	numErrors int
	// +checklocks:mu
	errors []error
}

func oidOf(e fs.Entry) object.ID {
	if h, ok := e.(object.HasObjectID); ok {
		return h.ObjectID()
	}

	return object.EmptyID
}

// ReportError reports the error.
func (w *TreeWalker) ReportError(ctx context.Context, entryPath string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	repoFSLog(ctx).Errorf("error processing %v: %v", entryPath, err)

	if len(w.errors) < w.options.MaxErrors {
		w.errors = append(w.errors, err)
	}

	w.numErrors++
}

// Err returns the error encountered when walking the tree.
func (w *TreeWalker) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.numErrors {
	case 0:
		return nil
	case 1:
		return w.errors[0]
	default:
		return errors.Errorf("encountered %v errors", w.numErrors)
	}
}

// TooManyErrors reports true if there are too many errors already reported.
func (w *TreeWalker) TooManyErrors() bool {
	if w.options.MaxErrors <= 0 {
		// unlimited
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.numErrors >= w.options.MaxErrors
}

func (w *TreeWalker) alreadyProcessed(e fs.Entry) bool {
	_, existing := w.enqueued.LoadOrStore(oidOf(e), struct{}{})
	return existing
}

func (w *TreeWalker) processEntry(ctx context.Context, e fs.Entry, entryPath string) {
	if ec := w.options.EntryCallback; ec != nil {
		err := ec(ctx, e, oidOf(e), entryPath)
		if err != nil {
			w.ReportError(ctx, entryPath, err)
			return
		}
	}

	if dir, ok := e.(fs.Directory); ok {
		w.processDirEntry(ctx, dir, entryPath)
	}
}

func (w *TreeWalker) processDirEntry(ctx context.Context, dir fs.Directory, entryPath string) {
	var ag workshare.AsyncGroup
	defer ag.Wait()

	entries, err := dir.Readdir(ctx)
	if err != nil {
		w.ReportError(ctx, entryPath, errors.Wrap(err, "error reading directory"))
		return
	}

	for _, ent := range entries {
		ent := ent

		if w.TooManyErrors() {
			break
		}

		if w.alreadyProcessed(ent) {
			continue
		}

		childPath := path.Join(entryPath, ent.Name())

		if ag.CanShareWork(w.wp) {
			ag.RunAsync(w.wp, func(c *workshare.Pool, request interface{}) {
				w.processEntry(ctx, ent, childPath)
			}, nil)
		} else {
			w.processEntry(ctx, ent, childPath)
		}
	}
}

// Process processes the snapshot tree entry.
func (w *TreeWalker) Process(ctx context.Context, e fs.Entry, entryPath string) error {
	if oidOf(e) == object.EmptyID {
		return errors.Errorf("entry does not have ObjectID")
	}

	if w.alreadyProcessed(e) {
		return nil
	}

	w.processEntry(ctx, e, entryPath)

	return w.Err()
}

// Close closes the tree walker.
func (w *TreeWalker) Close() {
	w.wp.Close()
}

// TreeWalkerOptions provides optional fields for TreeWalker.
type TreeWalkerOptions struct {
	EntryCallback EntryCallback

	Parallelism int
	MaxErrors   int
}

// NewTreeWalker creates new tree walker.
func NewTreeWalker(options TreeWalkerOptions) *TreeWalker {
	if options.Parallelism <= 0 {
		options.Parallelism = runtime.NumCPU() * walkersPerCPU
	}

	if options.MaxErrors == 0 {
		options.MaxErrors = 1
	}

	return &TreeWalker{
		options: options,
		wp:      workshare.NewPool(options.Parallelism - 1),
	}
}
