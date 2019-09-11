package snapshotfs

import (
	"context"
	"runtime"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo/object"
)

type TreeWalker struct {
	Parallelism    int
	RootEntries    []fs.Entry
	ObjectCallback func(oid object.ID) error

	enqueued sync.Map
	queue    *parallelwork.Queue
}

func oidOf(entry fs.Entry) object.ID {
	return entry.(object.HasObjectID).ObjectID()
}

func (w *TreeWalker) enqueueEntry(ctx context.Context, entry fs.Entry) {
	oid := oidOf(entry)
	if _, existing := w.enqueued.LoadOrStore(oid, w); existing {
		return
	}

	w.queue.EnqueueBack(func() error { return w.processEntry(ctx, entry) })
}

func (w *TreeWalker) processEntry(ctx context.Context, entry fs.Entry) error {
	err := w.ObjectCallback(oidOf(entry))
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

func (w *TreeWalker) Run(ctx context.Context) error {
	for _, root := range w.RootEntries {
		w.enqueueEntry(ctx, root)
	}
	w.queue.ProgressCallback = func(enqueued, active, completed int64) {
		log.Infof("processed(%v/%v) active %v", completed, enqueued, active)

	}
	return w.queue.Process(w.Parallelism)
}

// NewTreeWalker creates new tree walker.
func NewTreeWalker() *TreeWalker {
	return &TreeWalker{
		Parallelism: 4 * runtime.NumCPU(),
		queue:       parallelwork.NewQueue(),
	}
}
