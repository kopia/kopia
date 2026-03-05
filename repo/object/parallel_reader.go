package object

import (
	"context"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ErrNotParallelizable is returned when an object is not a multi-chunk indirect
// object and thus cannot be fetched in parallel.
var ErrNotParallelizable = errors.New("object does not support parallel chunk reading")

// DefaultParallelChunkWorkers is the default number of concurrent chunk fetchers
// used when parallel chunk restore is enabled.
const DefaultParallelChunkWorkers = 8

// objectOpener can open objects by ID; satisfied by repo.Repository.
type objectOpener interface {
	OpenObject(ctx context.Context, id ID) (Reader, error)
}

// ReadIndirectObjectParallel reads the chunks of an indirect object in parallel,
// calling callback(startOffset, data) for each chunk (possibly out of order).
// Returns ErrNotParallelizable if oid is not a multi-chunk indirect object.
func ReadIndirectObjectParallel(ctx context.Context, opener objectOpener, oid ID, workers int, callback func(offset int64, data []byte) error) error {
	indexObjectID, ok := oid.IndexObjectID()
	if !ok {
		return ErrNotParallelizable
	}

	seekTable, err := loadSeekTableViaOpener(ctx, opener, indexObjectID)
	if err != nil {
		return err
	}

	if len(seekTable) <= 1 {
		return ErrNotParallelizable
	}

	if workers <= 0 {
		workers = DefaultParallelChunkWorkers
	}

	jobs := make(chan int, len(seekTable))
	for i := range seekTable {
		jobs <- i
	}

	close(jobs)

	g, gctx := errgroup.WithContext(ctx)

	for range workers {
		g.Go(func() error {
			for idx := range jobs {
				entry := seekTable[idx]

				r, err := opener.OpenObject(gctx, entry.Object)
				if err != nil {
					return errors.Wrapf(err, "opening chunk %d", idx)
				}

				b := make([]byte, entry.Length)
				_, err = io.ReadFull(r, b)
				r.Close() //nolint:errcheck

				if err != nil {
					return errors.Wrapf(err, "reading chunk %d", idx)
				}

				if err := callback(entry.Start, b); err != nil {
					return err
				}
			}

			return nil
		})
	}

	return g.Wait()
}

func loadSeekTableViaOpener(ctx context.Context, opener objectOpener, indexID ID) ([]IndirectObjectEntry, error) {
	r, err := opener.OpenObject(ctx, indexID)
	if err != nil {
		return nil, errors.Wrapf(err, "opening index object %v", indexID)
	}

	defer r.Close() //nolint:errcheck

	var ind indirectObject
	if err := json.NewDecoder(r).Decode(&ind); err != nil {
		return nil, errors.Wrap(err, "invalid indirect object")
	}

	return ind.Entries, nil
}
