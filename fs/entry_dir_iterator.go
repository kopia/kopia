package fs

import "context"

type staticIterator struct {
	cur     int
	entries []Entry
	err     error
}

func (it *staticIterator) Close() {
}

func (it *staticIterator) Next(ctx context.Context) (Entry, error) {
	if it.cur < len(it.entries) {
		v := it.entries[it.cur]
		it.cur++

		return v, it.err
	}

	return nil, nil
}

// StaticIterator returns a DirectoryIterator which returns the provided
// entries in order followed by a given final error.
// It is not safe to concurrently access directory iterator.
func StaticIterator(entries []Entry, err error) DirectoryIterator {
	return &staticIterator{0, entries, err}
}
