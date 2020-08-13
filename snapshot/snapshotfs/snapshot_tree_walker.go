package snapshotfs

import (
	"context"
	"runtime"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/parallelwork"
)

const walkersPerCPU = 4

// TreeWalker holds information for concurrently walking down FS trees specified
// by their roots.
type TreeWalker struct {
	Parallelism    int
	RootEntries    []fs.Entry
	ObjectCallback func(entry fs.Entry) error
	// EntryID extracts or generates an id from an fs.Entry.
	// It can be used to eliminate duplicate entries when in a FS
	EntryID func(entry fs.Entry) interface{}

	enqueued sync.Map
	queue    *parallelwork.Queue
}

func (w *TreeWalker) enqueueEntry(ctx context.Context, entry fs.Entry) {
	eid := w.EntryID(entry)
	if _, existing := w.enqueued.LoadOrStore(eid, w); existing {
		return
	}

	w.queue.EnqueueBack(func() error { return w.processEntry(ctx, entry) })
}

func (w *TreeWalker) processEntry(ctx context.Context, entry fs.Entry) error {
	err := w.ObjectCallback(entry)
	if err != nil {
		return err
	}

	if dir, ok := entry.(fs.Directory); ok {
		entries, err := dir.Readdir(ctx)
		if err != nil {
			return errors.Wrap(err, "error reading directory")
		}

		for _, ent := range entries {
			w.enqueueEntry(ctx, ent)
		}
	}

	return nil
}

// Run walks the given tree roots.
func (w *TreeWalker) Run(ctx context.Context) error {
	for _, root := range w.RootEntries {
		w.enqueueEntry(ctx, root)
	}

	w.queue.ProgressCallback = func(enqueued, active, completed int64) {
		log(ctx).Infof("processed(%v/%v) active %v", completed, enqueued, active)
	}

	return w.queue.Process(w.Parallelism)
}

// NewTreeWalker creates new tree walker.
func NewTreeWalker() *TreeWalker {
	return &TreeWalker{
		Parallelism: walkersPerCPU * runtime.NumCPU(),
		queue:       parallelwork.NewQueue(),
	}
}
