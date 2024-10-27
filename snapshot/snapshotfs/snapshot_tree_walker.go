package snapshotfs

import (
	"context"
	"path"
	"runtime"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/bigmap"
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

	enqueued *bigmap.Set
	wp       *workshare.Pool[any]

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

	// Record one error if we can't get too many errors so that at least that one
	// can be returned if it's the only one.
	if len(w.errors) < w.options.MaxErrors || (w.options.MaxErrors <= 0 && len(w.errors) == 0) {
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

func (w *TreeWalker) alreadyProcessed(ctx context.Context, e fs.Entry) bool {
	var idbuf [128]byte

	return !w.enqueued.Put(ctx, oidOf(e).Append(idbuf[:0]))
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
	var ag workshare.AsyncGroup[any]
	defer ag.Close()

	iter, err := dir.Iterate(ctx)
	if err != nil {
		w.ReportError(ctx, entryPath, errors.Wrap(err, "error reading directory"))

		return
	}

	defer iter.Close()

	ent, err := iter.Next(ctx)
	for ent != nil {
		ent2 := ent

		if w.TooManyErrors() {
			break
		}

		if !w.alreadyProcessed(ctx, ent2) {
			childPath := path.Join(entryPath, ent2.Name())

			if ag.CanShareWork(w.wp) {
				ag.RunAsync(w.wp, func(_ *workshare.Pool[any], _ any) {
					w.processEntry(ctx, ent2, childPath)
				}, nil)
			} else {
				w.processEntry(ctx, ent2, childPath)
			}
		}

		ent, err = iter.Next(ctx)
	}

	if err != nil {
		w.ReportError(ctx, entryPath, errors.Wrap(err, "error reading directory"))
	}
}

// Process processes the snapshot tree entry.
func (w *TreeWalker) Process(ctx context.Context, e fs.Entry, entryPath string) error {
	if oidOf(e) == object.EmptyID {
		return errors.New("entry does not have ObjectID")
	}

	if w.alreadyProcessed(ctx, e) {
		return nil
	}

	w.processEntry(ctx, e, entryPath)

	return w.Err()
}

// Close closes the tree walker.
func (w *TreeWalker) Close(ctx context.Context) {
	w.wp.Close()
	w.enqueued.Close(ctx)
}

// TreeWalkerOptions provides optional fields for TreeWalker.
type TreeWalkerOptions struct {
	EntryCallback EntryCallback

	Parallelism int
	MaxErrors   int
}

// NewTreeWalker creates new tree walker.
func NewTreeWalker(ctx context.Context, options TreeWalkerOptions) (*TreeWalker, error) {
	if options.Parallelism <= 0 {
		options.Parallelism = runtime.NumCPU() * walkersPerCPU
	}

	if options.MaxErrors == 0 {
		options.MaxErrors = 1
	}

	s, err := bigmap.NewSet(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "NewSet")
	}

	return &TreeWalker{
		options:  options,
		wp:       workshare.NewPool[any](options.Parallelism - 1),
		enqueued: s,
	}, nil
}
