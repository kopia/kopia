package fs

import "context"

type staticIterator struct {
	cur     int
	entries []Entry
	err     error
}

func (it *staticIterator) Close() {
}

func (it *staticIterator) FinalErr() error {
	return it.err
}

func (it *staticIterator) Next(ctx context.Context) Entry {
	if it.cur < len(it.entries) {
		v := it.entries[it.cur]
		it.cur++

		return v
	}

	return nil
}

// StaticIterator returns a DirectoryIterator which returns the provided
// entries in order followed by a given final error.
func StaticIterator(entries []Entry, err error) DirectoryIterator {
	return &staticIterator{0, entries, err}
}
